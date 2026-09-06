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
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
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

// exclusiveOn is the allocation shape of every claim this driver prepares
// today: one request granting CPUs the claim holds alone.
func exclusiveOn(cpus cpuset.CPUSet) []store.RequestAllocation {
	return []store.RequestAllocation{{Request: "cpus", CPUs: cpus, Role: store.RoleExclusive}}
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

			err = mgr.AddDevice(logger, tc.deviceName, []string{tc.envVar}, exclusiveOn(cpuset.New(0, 1)))

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
						// Placement is recorded in a CDI annotation, which is
						// not injected into the container and so can be
						// rewritten while it runs.
						Annotations: map[string]string{cdiPlacementsAnnotation: `[{"request":"cpus","cpus":"0-1","role":"exclusive"}]`},
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

func TestRemoveDevice(t *testing.T) {
	testcases := []struct {
		name          string
		deviceName    string
		envVar        string
		simulateErr   bool
		expectedError string
	}{
		{
			name:        "successfully removes an existing device spec from disk",
			deviceName:  "claim-cpu-remove-success",
			envVar:      "CPU=4,5",
			simulateErr: false,
		},
		{
			name:          "fails to remove spec when directory is actually a file",
			deviceName:    "claim-cpu-remove-error",
			envVar:        "CPU=4,5",
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

			expectedSpecName := mgr.getSpecName(tc.deviceName)
			expectedFilePath := filepath.Join(tempCDIDir, expectedSpecName)

			if !tc.simulateErr {
				err = mgr.AddDevice(logger, tc.deviceName, []string{tc.envVar}, exclusiveOn(cpuset.New(0, 1)))
				require.NoError(t, err)
			}

			err = mgr.RemoveDevice(logger, tc.deviceName)

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

	err = mgr.AddDevice(logger, deviceName, []string{"CPU=0,1"}, exclusiveOn(cpuset.New(0, 1)))
	require.NoError(t, err)
	assertFileCount(1)

	// Verify the cache has the initial spec
	spec1 := getSpecFromCache(mgr, expectedSpecName)
	require.NotNil(t, spec1)
	require.Equal(t, []string{"CPU=0,1"}, spec1.Devices[0].ContainerEdits.Env)

	// Call AddDevice again with the same deviceName and same data
	err = mgr.AddDevice(logger, deviceName, []string{"CPU=0,1"}, exclusiveOn(cpuset.New(0, 1)))
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
	err = mgr.AddDevice(logger, deviceName, []string{expectedEnv}, exclusiveOn(cpuset.New(0, 1)))
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
	err = mgr.AddDevice(logger, deviceName, []string{expectedEnv}, exclusiveOn(cpuset.New(0, 1)))
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

func TestGetDeviceAllocations(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	deviceName := "claim-cpu-placement"
	require.NoError(t, mgr.AddDevice(logger, deviceName,
		[]string{"DRA_CPUSET_claim-cpu-placement=2-3"}, exclusiveOn(cpuset.New(2, 3))))
	require.NoError(t, mgr.Refresh())

	got, err := mgr.GetDeviceAllocations(deviceName)
	require.NoError(t, err)
	require.Equal(t, exclusiveOn(cpuset.New(2, 3)), got)

	// Rewriting the spec must move the recorded placement, since this is what
	// makes a claim's CPUs mutable while its container runs.
	require.NoError(t, mgr.AddDevice(logger, deviceName,
		[]string{"DRA_CPUSET_claim-cpu-placement=2-3"}, exclusiveOn(cpuset.New(6, 7))))
	require.NoError(t, mgr.Refresh())

	got, err = mgr.GetDeviceAllocations(deviceName)
	require.NoError(t, err)
	require.Equal(t, exclusiveOn(cpuset.New(6, 7)), got, "the annotation, not the env var, is the record")
}

func TestGetDeviceAllocationsFallsBackToEnv(t *testing.T) {
	logger := testr.New(t)
	tempCDIDir := t.TempDir()
	mgr, err := NewCdiManager(logger, testDriverName, tempCDIDir)
	require.NoError(t, err)

	// A spec written before the driver recorded placement in an annotation: the
	// env var is the only record, and unlike a container's environment the
	// driver-owned spec file's value is current.
	deviceName := "claim-cpu-legacy"
	legacy := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    cdiVendor + "/" + cdiClass,
		Devices: []cdiSpec.Device{{
			Name: deviceName,
			ContainerEdits: cdiSpec.ContainerEdits{
				Env: []string{"DRA_CPUSET_claim-cpu-legacy=4,5"},
			},
		}},
	}
	require.NoError(t, mgr.cache.WriteSpec(legacy, mgr.getSpecName(deviceName)))
	require.NoError(t, mgr.Refresh())

	got, err := mgr.GetDeviceAllocations(deviceName)
	require.NoError(t, err)
	require.Equal(t, []store.RequestAllocation{{CPUs: cpuset.New(4, 5), Role: store.RoleExclusive}}, got)
}

func TestGetDeviceAllocationsMissingDevice(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	_, err = mgr.GetDeviceAllocations("claim-cpu-absent")
	require.Error(t, err)
}

func TestPreparedClaimAllocationsRecoversRecordedPlacements(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	claimA := types.UID("claim-a")
	claimB := types.UID("claim-b")
	require.NoError(t, mgr.AddDevice(logger, getCDIDeviceName(claimA),
		[]string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimA, "0-1")}, exclusiveOn(cpuset.New(0, 1))))
	require.NoError(t, mgr.AddDevice(logger, getCDIDeviceName(claimB),
		[]string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimB, "dynamic")}, exclusiveOn(cpuset.New(4, 5))))
	require.NoError(t, mgr.Refresh())

	got := mgr.PreparedClaimAllocations(logger)
	require.Equal(t, map[types.UID][]store.RequestAllocation{
		claimA: exclusiveOn(cpuset.New(0, 1)),
		claimB: exclusiveOn(cpuset.New(4, 5)),
	}, got)
}

func TestPreparedClaimAllocationsEmptyWhenNothingOnDisk(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	require.Empty(t, mgr.PreparedClaimAllocations(logger))
}

func TestPreparedClaimAllocationsIgnoresDevicesThisDriverWouldNotHaveGenerated(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	// Neither is a "claim-<uid>" name, so recovering them would not name a real
	// claim UID -- a hand-edited or foreign spec sharing this driver's kind.
	foreign := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    cdiVendor + "/" + cdiClass,
		Devices: []cdiSpec.Device{{
			Name:           "not-a-claim-device",
			Annotations:    map[string]string{cdiCPUSetAnnotation: "2-3"},
			ContainerEdits: cdiSpec.ContainerEdits{Env: []string{"UNRELATED=1"}},
		}},
	}
	require.NoError(t, mgr.cache.WriteSpec(foreign, mgr.getSpecName("not-a-claim-device")))

	claimA := types.UID("claim-a")
	require.NoError(t, mgr.AddDevice(logger, getCDIDeviceName(claimA),
		[]string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimA, "0-1")}, exclusiveOn(cpuset.New(0, 1))))
	require.NoError(t, mgr.Refresh())

	got := mgr.PreparedClaimAllocations(logger)
	require.Equal(t, map[types.UID][]store.RequestAllocation{claimA: exclusiveOn(cpuset.New(0, 1))}, got)
}

func TestPreparedClaimAllocationsSkipsOneUnrecoverableDeviceWithoutFailingTheRest(t *testing.T) {
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	// A driver-written annotation that fails to parse: the spec was corrupted
	// or hand-edited, and this one claim's placement is unrecoverable, but that
	// must not cost every other claim its recovery too.
	corrupt := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    cdiVendor + "/" + cdiClass,
		Devices: []cdiSpec.Device{{
			Name:           "claim-corrupt",
			Annotations:    map[string]string{cdiCPUSetAnnotation: "not-a-cpuset"},
			ContainerEdits: cdiSpec.ContainerEdits{Env: []string{"UNRELATED=1"}},
		}},
	}
	require.NoError(t, mgr.cache.WriteSpec(corrupt, mgr.getSpecName("claim-corrupt")))

	claimA := types.UID("claim-a")
	require.NoError(t, mgr.AddDevice(logger, getCDIDeviceName(claimA),
		[]string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimA, "0-1")}, exclusiveOn(cpuset.New(0, 1))))
	require.NoError(t, mgr.Refresh())

	got := mgr.PreparedClaimAllocations(logger)
	require.Equal(t, map[types.UID][]store.RequestAllocation{claimA: exclusiveOn(cpuset.New(0, 1))}, got)
}

func TestGetDeviceAllocationsMalformedAnnotation(t *testing.T) {
	// The annotation is driver-written, so a value that does not parse means the
	// spec was corrupted or hand-edited. The claim's placement is then unknown,
	// which must surface as an error rather than as an empty cpuset a caller
	// would treat as "no CPUs".
	logger := testr.New(t)
	mgr, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)

	deviceName := "claim-cpu-corrupt"
	spec := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    cdiVendor + "/" + cdiClass,
		Devices: []cdiSpec.Device{{
			Name:        deviceName,
			Annotations: map[string]string{cdiCPUSetAnnotation: "not-a-cpuset"},
			ContainerEdits: cdiSpec.ContainerEdits{
				Env: []string{"DRA_CPUSET_claim-cpu-corrupt=0-1"},
			},
		}},
	}
	require.NoError(t, mgr.cache.WriteSpec(spec, mgr.getSpecName(deviceName)))
	require.NoError(t, mgr.Refresh())

	_, err = mgr.GetDeviceAllocations(deviceName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse")
	require.Contains(t, err.Error(), cdiCPUSetAnnotation)
}
