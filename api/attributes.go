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

package api

// Names are untyped constants so that a consumer can use them wherever a
// string-kind type is wanted, resourceapi.QualifiedName included, without this
// module depending on k8s.io/api.
//
// The name segment after the domain is validated as a C identifier of at most
// 32 characters, so a change of meaning takes a new name rather than a new
// value under the old one.
const (
	AttributeSocketID   = "dra.cpu/socketID"
	AttributeSMTEnabled = "dra.cpu/smtEnabled"
	AttributeCacheL3ID  = "dra.cpu/cacheL3ID"
	AttributeCoreType   = "dra.cpu/coreType"
	AttributeCoreID     = "dra.cpu/coreID"
	AttributeCPUID      = "dra.cpu/cpuID"
	AttributeNumCPUs    = "dra.cpu/numCPUs"
	// AttributeThreadsPerCore is a grouped device's own uniform thread-per-core
	// count (0 when its cores do not all agree), independent of node-wide SMT:
	// AttributeSMTEnabled is true exactly when this is greater than one.
	AttributeThreadsPerCore = "dra.cpu/threadsPerCore"
	// AttributeLargestUncoreCacheCPUs is the largest number of allocatable CPUs
	// sharing one uncore cache within a grouped device, i.e. the biggest claim
	// the group could satisfy from a single cache. It is not a per-cache size:
	// caches within a group can differ, so a claim asking for cache-aligned CPUs
	// must compare against the largest.
	AttributeLargestUncoreCacheCPUs = "dra.cpu/largestUncoreCacheCPUs"
	// AttributeUncoreCachesInGroup is how many uncore caches contribute
	// allocatable CPUs to a grouped device. One means cache alignment within the
	// group is trivially satisfied.
	AttributeUncoreCachesInGroup = "dra.cpu/uncoreCachesInGroup"
	// AttributePartition is the partition a device's CPUs belong to, which is
	// "default" on a node whose cores nobody has described.
	AttributePartition = "dra.cpu/partition"
	// AttributeRole is that partition's role, so a claim can select on what the
	// cores are for rather than on the name a fleet happened to give them.
	AttributeRole = "dra.cpu/role"
	// AttributeAllocatedNumCPUs is a metadata-only attribute (not published in
	// ResourceSlice) that indicates how many CPUs were allocated to a specific
	// claim from a grouped device's capacity.
	AttributeAllocatedNumCPUs = "dra.cpu/allocatedNumCPUs"
	// AttributeCPUSet is a metadata-only attribute naming the CPUs a request was
	// given, published for a request whose CPUs cannot change while its
	// container runs.
	AttributeCPUSet = "dra.cpu/cpuset"
	// AttributeRelocatable is a metadata-only attribute saying whether the claim
	// permits the driver to change its CPUs while its containers run, so a
	// workload can tell whether it has to watch its own cpuset.
	AttributeRelocatable = "dra.cpu/relocatable"
	// AttributeAlignment is a metadata-only attribute carrying what the claim
	// asked about landing split.
	AttributeAlignment = "dra.cpu/alignment"
)

// The repair frontier a driver publishes per NUMA node, which a scheduler reads
// to decide whether a claim that lands split can be made whole there.
const (
	// AttributeRepairRounds holds RepairRoundsFields comma-separated fields, one
	// per whole-cache claim size, each either a round count or
	// RepairRoundsUnreachable.
	AttributeRepairRounds = "dra.cpu/repairRounds"
	// AttributeFrontierInput digests the claims the frontier was computed over,
	// so a reader can tell a current frontier from one that predates its own
	// view of the node.
	AttributeFrontierInput = "dra.cpu/frontierInput"
	// RepairRoundsUnreachable is the field value for a size no bounded search
	// found a repair for.
	RepairRoundsUnreachable = "-"
	// RepairRoundsFields is how many fields AttributeRepairRounds carries.
	RepairRoundsFields = 4
)

// How a claim's requests constrain where the allocator may place it, which
// decides what the driver owes the claim once it is placed.
const (
	// ShapeNeverSplit is a claim whose only alternative is the aligned one, so
	// the allocator can never split it.
	ShapeNeverSplit = "never-split"
	// ShapeFlexible is a claim offering split alternatives.
	ShapeFlexible = "flexible"
)
