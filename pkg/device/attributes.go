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

package device

import (
	resourceapi "k8s.io/api/resource/v1"
)

const (
	// CPU_DEVICE_MODE_GROUPED exposes a single device for a group of CPUs.
	CPU_DEVICE_MODE_GROUPED = "grouped"
	// CPU_DEVICE_MODE_INDIVIDUAL exposes each CPU as a separate device.
	CPU_DEVICE_MODE_INDIVIDUAL = "individual"
)

const (
	// GROUP_BY_SOCKET groups CPUs by socket.
	GROUP_BY_SOCKET = "socket"
	// GROUP_BY_NUMA_NODE groups CPUs by NUMA node.
	GROUP_BY_NUMA_NODE = "numanode"
	// GROUP_BY_MACHINE groups CPUs by the entire machine.
	GROUP_BY_MACHINE = "machine"
	// GROUP_BY_UNCORE_CACHE groups CPUs by the uncore cache they share.
	GROUP_BY_UNCORE_CACHE = "uncorecache"
)

const (
	AttributeSocketID   resourceapi.QualifiedName = "dra.cpu/socketID"
	AttributeSMTEnabled resourceapi.QualifiedName = "dra.cpu/smtEnabled"
	AttributeCacheL3ID  resourceapi.QualifiedName = "dra.cpu/cacheL3ID"
	AttributeCoreType   resourceapi.QualifiedName = "dra.cpu/coreType"
	AttributeCoreID     resourceapi.QualifiedName = "dra.cpu/coreID"
	AttributeCPUID      resourceapi.QualifiedName = "dra.cpu/cpuID"
	AttributeNumCPUs    resourceapi.QualifiedName = "dra.cpu/numCPUs"
	// AttributeThreadsPerCore is a grouped device's threads per core, 0 when its cores differ.
	AttributeThreadsPerCore resourceapi.QualifiedName = "dra.cpu/threadsPerCore"
	// AttributeAllocatedNumCPUs is a metadata-only attribute (not published in
	// ResourceSlice) that indicates how many CPUs were allocated to a specific
	// claim from a grouped device's capacity.
	AttributeAllocatedNumCPUs resourceapi.QualifiedName = "dra.cpu/allocatedNumCPUs"
	AttributeCPUIDs           resourceapi.QualifiedName = "dra.cpu/cpuIDs"
	AttributeSMTLayout        resourceapi.QualifiedName = "dra.cpu/smtLayout"
	// AttributeFullPhysicalCPUsOnly and AttributeAllocator are the driver
	// settings of the same names, published on every device.
	AttributeFullPhysicalCPUsOnly resourceapi.QualifiedName = "dra.cpu/fullPhysicalCPUsOnly"
	AttributeAllocator            resourceapi.QualifiedName = "dra.cpu/allocator"
	// AttributeCPUSet is a metadata-only attribute that names the CPUs a request
	// holding exclusive CPUs was given on a device.
	AttributeCPUSet resourceapi.QualifiedName = "dra.cpu/cpuset"
	// AttributeCPURequest and AttributeCPULimit are metadata-only attributes
	// that mirror a container's CPU request and limit: the CPU amount booked for
	// a request on a device and the request's cap, as quantity strings, the
	// limit reading "max" where the request has no cap.
	AttributeCPURequest resourceapi.QualifiedName = "dra.cpu/cpuRequest"
	AttributeCPULimit   resourceapi.QualifiedName = "dra.cpu/cpuLimit"
)

// addCompatibilityAttributes add attributes to enable compatibility (e.g. alignment) with other
// DRA resource drivers leveraging attributes which are not kubernetes standard.
// This is the "staging area" which enables attribute sharing until (or before) they become standard.
func addCompatibilityAttributes(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, numaID int64) {
	attrs["dra.net/numaNode"] = resourceapi.DeviceAttribute{IntValue: new(numaID)}
	attrs["dra.cpu/numaNodeID"] = resourceapi.DeviceAttribute{IntValue: new(numaID)}
}

// addSettingAttributes publishes the settings Prepare checks a claim against,
// so that a claim's selector can keep it off a node whose Prepare would refuse
// it after the scheduler placed it there.
func addSettingAttributes(devices []resourceapi.Device, fullPhysicalCPUsOnly bool, allocator string) {
	for _, dev := range devices {
		dev.Attributes[AttributeFullPhysicalCPUsOnly] = resourceapi.DeviceAttribute{BoolValue: new(fullPhysicalCPUsOnly)}
		dev.Attributes[AttributeAllocator] = resourceapi.DeviceAttribute{StringValue: new(allocator)}
	}
}
