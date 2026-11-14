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

package driverconfig

import "testing"

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		conf        Config
		expectedErr bool
	}{{
		name: "error: unknown allocator",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "other-allocator",
		},
		expectedErr: true,
	}, {
		name: "error: non-external allocator in machine mode",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "machine",
			Allocator:      "cpumanager",
		},
		expectedErr: true,
	}, {
		name: "valid: external allocator in machine mode",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "machine",
			Allocator:      "external",
		},
		expectedErr: false,
	}, {
		name: "valid: allocator: cpumanager",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
		},
		expectedErr: false,
	}, {
		name: "valid: allocator: external",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "external",
		},
		expectedErr: false,
	}, {
		name: "error: zero qps",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			Burst:          50,
		},
		expectedErr: true,
	}, {
		name: "error: negative qps",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            -1,
			Burst:          50,
		},
		expectedErr: true,
	}, {
		name: "error: zero burst",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            20,
		},
		expectedErr: true,
	}, {
		name: "valid: fractional qps, burst of one",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            0.5,
			Burst:          1,
		},
		expectedErr: false,
	}, {
		name: "valid: fullPhysicalCPUsOnly in grouped mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "grouped",
			GroupBy:              "numanode",
			Allocator:            "cpumanager",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: false,
	}, {
		name: "valid: fullPhysicalCPUsOnly with the external allocator in machine mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "grouped",
			GroupBy:              "machine",
			Allocator:            "external",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: false,
	}, {
		name: "error: fullPhysicalCPUsOnly in individual mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "individual",
			Allocator:            "cpumanager",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.conf.Validate()
			gotErr := (err != nil)
			if gotErr != tc.expectedErr {
				t.Errorf("error got=%v expected=%v (err=%v)", gotErr, tc.expectedErr, err)
			}
		})
	}
}
