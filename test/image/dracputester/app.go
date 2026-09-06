//go:build linux

/*
Copyright 2025 The Kubernetes Authors.

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

package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/stdr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/discovery"
	"k8s.io/utils/cpuset"
)

const (
	cgroupPath = "fs/cgroup"
	cpusetFile = "cpuset.cpus.effective"
	// metadataRoot is where the kubelet mounts one KEP-5304 metadata file per
	// request of every claim the container holds.
	metadataRoot = "/var/run/kubernetes.io/dra-device-attributes"
	// affinityScanMax is the upper bound when scanning sched_getaffinity if topology is unavailable.
	// Using runtime.NumCPU() would miss CPUs when the cgroup cpuset is non-contiguous (e.g. 2-5,9-13).
	affinityScanMax = 2048
)

func cpuSetPath() string {
	return filepath.Join(cgroupPath, cpusetFile)
}

func cpuSet(sfs fs.FS) (cpuset.CPUSet, error) {
	data, err := fs.ReadFile(sfs, cpuSetPath())
	if err != nil {
		return cpuset.New(), err
	}
	return cpuset.Parse(strings.TrimSpace(string(data)))
}

// affinityMask is satisfied by unix.CPUSet and test doubles.
type affinityMask interface {
	IsSet(i int) bool
}

// affinityFromMask scans the mask from 0 to maxCPUID (exclusive) and returns the set of set CPU IDs.
// If maxCPUID <= 0, affinityScanMax is used.
func affinityFromMask(mask affinityMask, maxCPUID int) cpuset.CPUSet {
	if maxCPUID <= 0 {
		maxCPUID = affinityScanMax
	}
	var allowedCPUs []int
	for i := 0; i < maxCPUID; i++ {
		if mask.IsSet(i) {
			allowedCPUs = append(allowedCPUs, i)
		}
	}
	return cpuset.New(allowedCPUs...)
}

func affinityScanBoundFromTopology(topo *cpuinfo.CPUTopology) int {
	if topo == nil || topo.NumCPUs == 0 {
		return affinityScanMax
	}
	cpus := topo.CPUDetails.CPUs()
	if cpus.Size() == 0 {
		return affinityScanMax
	}
	list := cpus.List()
	return list[len(list)-1] + 1
}

// deviceMetadata mirrors the fields of the metadata file this test reads. It
// is deliberately a local shape rather than the library type: the point is to
// exercise the file as a workload sees it.
type deviceMetadata struct {
	Requests []struct {
		Name    string `json:"name"`
		Devices []struct {
			Name       string `json:"name"`
			Attributes map[string]struct {
				String *string `json:"string"`
			} `json:"attributes"`
		} `json:"devices"`
	} `json:"requests"`
}

// requestMetadata reports what every mounted metadata file says, one entry per
// device of every request. The file is a stream of the same object once per
// API version, newest first, so only the first is decoded.
func requestMetadata() []discovery.DRACPURequestMetadata {
	claims, err := os.ReadDir(metadataRoot)
	if err != nil {
		return nil
	}
	var entries []discovery.DRACPURequestMetadata
	for _, claim := range claims {
		requests, err := os.ReadDir(filepath.Join(metadataRoot, claim.Name()))
		if err != nil {
			continue
		}
		for _, request := range requests {
			raw, err := os.ReadFile(filepath.Join(metadataRoot, claim.Name(), request.Name(), "metadata.json"))
			if err != nil {
				continue
			}
			var metadata deviceMetadata
			if err := json.NewDecoder(strings.NewReader(string(raw))).Decode(&metadata); err != nil {
				continue
			}
			for _, req := range metadata.Requests {
				if req.Name != request.Name() {
					continue
				}
				for _, dev := range req.Devices {
					entry := discovery.DRACPURequestMetadata{Claim: claim.Name(), Request: req.Name}
					if v, ok := dev.Attributes["dra.cpu/partition"]; ok && v.String != nil {
						entry.Partition = *v.String
					}
					if v, ok := dev.Attributes["dra.cpu/role"]; ok && v.String != nil {
						entry.Role = *v.String
					}
					if v, ok := dev.Attributes["dra.cpu/cpuset"]; ok && v.String != nil {
						entry.CPUs = *v.String
					}
					entries = append(entries, entry)
				}
			}
		}
	}
	return entries
}

func main() {
	logger := stdr.New(log.Default())
	// Read the container's cgroup view, intentionally ignoring HOST_ROOT.
	containerSysfs := os.DirFS("/sys")
	for {
		cpus, err := cpuSet(containerSysfs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error determining allocated cpus: %v\n", err)
			os.Exit(1)
		}
		cpuAff, err := getAffinity(logger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error determining CPU affinity: %v\n", err)
			os.Exit(2)
		}
		info := discovery.DRACPUTester{
			Buildinfo: discovery.NewBuildinfo(),
			Allocation: discovery.DRACPUAllocation{
				CPUs: cpus.String(),
			},
			Runtimeinfo: discovery.DRACPURuntimeinfo{
				CPUAffinity: cpuAff.String(),
			},
		}
		err = json.NewEncoder(os.Stdout).Encode(info)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error encoding info: %v\n", err)
			os.Exit(2)
		}

		time.Sleep(5 * time.Second)
	}
}
