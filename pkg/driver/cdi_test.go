/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
	cdiSpec "tags.cncf.io/container-device-interface/specs-go"
)

// getSpecFromCache is a test helper to verify WriteSpec succeeded.
// It forces a cache refresh and searches for the specific dynamically generated filename.
func getSpecFromCache(mgr *CdiManager, targetSpecName string) *cdiSpec.Spec {
	_ = mgr.cache.Refresh()
	specs := mgr.cache.GetVendorSpecs(cdiVendor)
	for _, spec := range specs {
		if spec.GetClass() == cdiClass && filepath.Base(spec.GetPath()) == targetSpecName {
			return spec.Spec
		}
	}
	return nil
}

func TestAddDevice(t *testing.T) {
	testcases := []struct {
		name          string
		deviceName    string
		envVar        string
		simulateErr   bool
		expectedError string
	}{
		{
			name:        "successfully writes a new device spec to disk",
			deviceName:  "claim-cpu-add-success",
			envVar:      "CPU=2,3",
			simulateErr: false,
		},
		{
			name:          "fails to writes pec to disk",
			deviceName:    "claim-cpu-add-error",
			envVar:        "CPU=2,3",
			simulateErr:   true,
			expectedError: "failed to write CDI spec",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			logger := testr.New(t)
			tempCDIDir := t.TempDir()

			if tc.simulateErr {
				tempFile := filepath.Join(tempCDIDir, "invalid-dir-file")
				err := os.WriteFile(tempFile, []byte(""), 0600)
				require.NoError(t, err)
				tempCDIDir = tempFile
			}

			mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
			require.NoError(t, err)

			expectedSpecName := mgr.getSpecName(tc.deviceName)
			expectedFilePath := filepath.Join(tempCDIDir, expectedSpecName)

			err = mgr.AddDevice(logger, tc.deviceName, tc.envVar)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)

			_, err = os.Stat(expectedFilePath)
			require.NoError(t, err, "expected CDI spec file to be created on disk")

			expectedSpec := &cdiSpec.Spec{
				Version: cdiSpecVersion,
				Kind:    cdiVendor + "/" + cdiClass,
				Devices: []cdiSpec.Device{
					{
						Name: tc.deviceName,
						ContainerEdits: cdiSpec.ContainerEdits{
							Env: []string{tc.envVar},
						},
					},
				},
			}

			got := getSpecFromCache(mgr, expectedSpecName)
			if diff := cmp.Diff(expectedSpec, got); diff != "" {
				t.Errorf("unexpected spec diff: %v", diff)
			}
		})
	}
}

func testClaimRecord() cdiClaimRecord {
	four := resource.MustParse("4")
	return cdiClaimRecord{
		Namespace: "ns",
		Name:      "vm-claim",
		Requests: []cdiRequestRecord{
			{Request: "helpers", CPUs: cpuset.New(8), Results: []cdiResultRecord{{Device: "cpudev008"}}},
			{Request: "vcpus", CPUs: cpuset.New(0, 1, 2, 3), Results: []cdiResultRecord{{Device: "cpudevnuma000", Consumed: &four}}},
		},
	}
}

func testClaimDeviceEnvs(claimUID types.UID) map[string]string {
	return map[string]string{
		getCDIRequestDeviceName(claimUID, "vcpus"):   fmt.Sprintf("%s_%s_vcpus=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue),
		getCDIRequestDeviceName(claimUID, "helpers"): fmt.Sprintf("%s_%s_helpers=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue),
	}
}

func TestAddClaimDevices(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()
	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	claimUID := types.UID("vm")
	envs := testClaimDeviceEnvs(claimUID)
	require.NoError(t, mgr.AddClaimDevices(logger, claimUID, envs, testClaimRecord()))
	require.NoError(t, mgr.AddClaimDevices(logger, claimUID, envs, testClaimRecord()))
	files, err := os.ReadDir(tempCDIDir)
	require.NoError(t, err)
	require.Len(t, files, 1, "one spec file holds every device of a claim")

	spec := getSpecFromCache(mgr, mgr.getSpecName(getCDIDeviceName(claimUID)))
	require.NotNil(t, spec)
	require.Equal(t, []string{getCDIRequestDeviceName(claimUID, "helpers"), getCDIRequestDeviceName(claimUID, "vcpus")},
		[]string{spec.Devices[0].Name, spec.Devices[1].Name}, "devices in the order of their names")
	require.Empty(t, spec.ContainerEdits, "nothing of the spec itself reaches a container")
	require.Equal(t, map[string]string{
		cdiRecordAnnotation: `{"namespace":"ns","name":"vm-claim","placements":[{"request":"helpers","cpus":"8","results":[{"device":"cpudev008"}]},{"request":"vcpus","cpus":"0-3","results":[{"device":"cpudevnuma000","consumed":"4"}]}]}`,
	}, spec.Annotations, "the record is one annotation")

	record, err := mgr.GetDeviceAllocations(claimUID)
	require.NoError(t, err)
	require.Equal(t, testClaimRecord(), record, "the record reads back as written")
	for deviceName, env := range envs {
		got, err := mgr.GetDeviceEnv(deviceName)
		require.NoError(t, err)
		require.Equal(t, []string{env}, got, "device %q resolves by name in a spec holding several", deviceName)
	}
}

func TestGetDeviceAllocations(t *testing.T) {
	claimUID := types.UID("vm")
	specWith := func(annotations map[string]string, env string) *cdiSpec.Spec {
		return &cdiSpec.Spec{
			Version:     cdiSpecVersion,
			Kind:        cdiVendor + "/" + cdiClass,
			Annotations: annotations,
			Devices: []cdiSpec.Device{{
				Name:           getCDIDeviceName(claimUID),
				ContainerEdits: cdiSpec.ContainerEdits{Env: []string{env}},
			}},
		}
	}
	record := func(value string) map[string]string {
		return map[string]string{cdiRecordAnnotation: value}
	}
	tests := []struct {
		name    string
		spec    *cdiSpec.Spec
		want    cdiClaimRecord
		wantErr string
	}{
		{
			name: "upstream's spec is one request without a name",
			spec: specWith(nil, "DRA_CPUSET_vm=0-1"),
			want: cdiClaimRecord{Requests: []cdiRequestRecord{{CPUs: cpuset.New(0, 1)}}},
		},
		{
			name:    "no spec of the claim",
			wantErr: "failed to find CDI spec",
		},
		{
			name:    "a record without the claim's namespace",
			spec:    specWith(record(`{"name":"vm-claim","placements":[{"request":"vcpus","cpus":"0"}]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: "dra.cpu/record records no namespace of the claim",
		},
		{
			name:    "a record without the claim's name",
			spec:    specWith(record(`{"namespace":"ns","name":"","placements":[{"request":"vcpus","cpus":"0"}]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: "dra.cpu/record records no name of the claim",
		},
		{
			name:    "a record of no request",
			spec:    specWith(record(`{"namespace":"ns","name":"vm-claim","placements":[]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: "dra.cpu/record records no request",
		},
		{
			name:    "a null list of requests",
			spec:    specWith(record(`{"namespace":"ns","name":"vm-claim","placements":null}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: "dra.cpu/record records no request",
		},
		{
			name:    "a request recorded twice",
			spec:    specWith(record(`{"namespace":"ns","name":"vm-claim","placements":[{"request":"vcpus","cpus":"0"},{"request":"vcpus","cpus":"1"}]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: `dra.cpu/record records request "vcpus" twice or without a name`,
		},
		{
			name:    "a request without CPUs",
			spec:    specWith(record(`{"namespace":"ns","name":"vm-claim","placements":[{"request":"vcpus","cpus":""}]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: `dra.cpu/record records no CPU for request "vcpus"`,
		},
		{
			name:    "CPUs that do not parse",
			spec:    specWith(record(`{"namespace":"ns","name":"vm-claim","placements":[{"request":"vcpus","cpus":"a-b"}]}`), "DRA_CPUSET_vm_vcpus=dynamic"),
			wantErr: `failed to parse the CPUs of request "vcpus"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := testr.New(t)
			mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
			require.NoError(t, err)
			if tc.spec != nil {
				require.NoError(t, mgr.cache.WriteSpec(tc.spec, mgr.getSpecName(getCDIDeviceName(claimUID))))
			}
			require.NoError(t, mgr.Refresh())

			record, err := mgr.GetDeviceAllocations(claimUID)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, record)
		})
	}
}

func TestListClaims(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, mgr.AddClaimDevices(logger, "vm", testClaimDeviceEnvs("vm"), testClaimRecord()))
	require.NoError(t, mgr.AddDevice(logger, getCDIDeviceName("up"), "DRA_CPUSET_up=0-1"))
	other := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    cdiVendor + "/" + cdiClass,
		Devices: []cdiSpec.Device{{Name: "other", ContainerEdits: cdiSpec.ContainerEdits{Env: []string{"OTHER=1"}}}},
	}
	require.NoError(t, mgr.cache.WriteSpec(other, mgr.getSpecName("other")))
	require.NoError(t, mgr.Refresh())

	require.Equal(t, []types.UID{"up", "vm"}, mgr.ListClaims(), "the claims of both forms, in order, and no spec named otherwise")
}

func TestRemoveClaimDevices(t *testing.T) {
	claimUID := types.UID("vm")
	testcases := []struct {
		name          string
		add           func(*CdiManager) error
		simulateErr   bool
		expectedError string
	}{
		{
			name: "removes the devices and the record of a claim",
			add: func(mgr *CdiManager) error {
				return mgr.AddClaimDevices(testr.New(t), claimUID, testClaimDeviceEnvs(claimUID), testClaimRecord())
			},
		},
		{
			name: "removes a claim in upstream's form",
			add: func(mgr *CdiManager) error {
				return mgr.AddDevice(testr.New(t), getCDIDeviceName(claimUID), "DRA_CPUSET_vm=0-1")
			},
		},
		{
			name: "a missing spec counts as removed",
		},
		{
			name:          "fails to remove spec when directory is actually a file",
			simulateErr:   true,
			expectedError: "failed to remove CDI spec",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			logger := testr.New(t)
			tempCDIDir := t.TempDir()

			if tc.simulateErr {
				tempFile := filepath.Join(tempCDIDir, "invalid-dir-file")
				err := os.WriteFile(tempFile, []byte(""), 0600)
				require.NoError(t, err)
				tempCDIDir = tempFile
			}

			mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
			require.NoError(t, err)

			expectedSpecName := mgr.getSpecName(getCDIDeviceName(claimUID))
			expectedFilePath := filepath.Join(tempCDIDir, expectedSpecName)

			if tc.add != nil {
				require.NoError(t, tc.add(mgr))
			}

			err = mgr.RemoveClaimDevices(logger, claimUID)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)

			_, err = os.Stat(expectedFilePath)
			require.Error(t, err, "expected an error when stating a deleted file")
			require.True(t, os.IsNotExist(err), "expected file to not exist on disk, but got: %v", err)

			gotAfterRemove := getSpecFromCache(mgr, expectedSpecName)
			require.Nil(t, gotAfterRemove, "expected spec to be nil in cache after removal")
		})
	}
}

func TestAddDeviceOverwrite(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()

	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	deviceName := "claim-cpu-overwrite"
	expectedSpecName := mgr.getSpecName(deviceName)

	assertFileCount := func(expected int) {
		files, err := os.ReadDir(tempCDIDir)
		require.NoError(t, err)
		require.Len(t, files, expected)
	}

	err = mgr.AddDevice(logger, deviceName, "CPU=0,1")
	require.NoError(t, err)
	assertFileCount(1)

	// Verify the cache has the initial spec
	spec1 := getSpecFromCache(mgr, expectedSpecName)
	require.NotNil(t, spec1)
	require.Equal(t, []string{"CPU=0,1"}, spec1.Devices[0].ContainerEdits.Env)

	// Call AddDevice again with the same deviceName and same data
	err = mgr.AddDevice(logger, deviceName, "CPU=0,1")
	require.NoError(t, err)
	// Verify that we do not create a new file
	assertFileCount(1)
}

func TestGetDeviceEnv(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()

	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	deviceName := "claim-cpu-get-env"
	expectedEnv := "DRA_CPUSET_claim-cpu-get-env=0,1"
	err = mgr.AddDevice(logger, deviceName, expectedEnv)
	require.NoError(t, err)
	err = mgr.Refresh()
	require.NoError(t, err)

	envs, err := mgr.GetDeviceEnv(deviceName)
	require.NoError(t, err)
	require.Equal(t, []string{expectedEnv}, envs)
}

func TestRefreshKeepsValidDevicesWhenAnotherSpecIsInvalid(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()

	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	deviceName := "claim-cpu-valid"
	expectedEnv := "DRA_CPUSET_claim-cpu-valid=0,1"
	err = mgr.AddDevice(logger, deviceName, expectedEnv)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(tempCDIDir, "unrelated-invalid.json"), []byte("{"), 0600)
	require.NoError(t, err)
	require.Error(t, mgr.Refresh())

	envs, err := mgr.GetDeviceEnv(deviceName)
	require.NoError(t, err)
	require.Equal(t, []string{expectedEnv}, envs)
}

func TestGetDeviceEnvMissingDevice(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()

	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	_, err = mgr.GetDeviceEnv("missing-device")
	require.Error(t, err)
	require.Contains(t, err.Error(), `failed to find CDI device "missing-device"`)
}
