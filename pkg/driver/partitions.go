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
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"
)

const conditionPartitionDegraded corev1.NodeConditionType = "CPUPartitionDegraded"

var partitionsVerified = nodeConditionValue{
	status:  corev1.ConditionFalse,
	reason:  "PartitionsVerified",
	message: "the kernel has online the threads per core every CPU partition's smt declares",
}

// resolvePartitions checks the declared partitions against the node and
// resolves them. Without declared partitions the node is the one partition
// device.DefaultPartitionName, as upstream's node is. It also returns the
// partitions whose smt the kernel contradicts, by name, each with what
// contradicts it.
func resolvePartitions(logger logr.Logger, sysfs fs.ReadLinkFS, topo *cpuinfo.CPUTopology, declared []device.Partition, reservedCPUs cpuset.CPUSet) ([]device.Partition, map[string]string, error) {
	if len(declared) == 0 {
		return device.ResolvePartitions(nil, topo.CPUDetails, reservedCPUs), nil, nil
	}
	presentCPUs, err := cpuinfo.PresentCPUs(sysfs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the present CPUs: %w", err)
	}
	onlineCPUs, err := cpuinfo.OnlineCPUs(sysfs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the online CPUs: %w", err)
	}
	if err := validatePartitions(topo.CPUDetails, presentCPUs, onlineCPUs, declared); err != nil {
		return nil, nil, err
	}
	partitions := device.ResolvePartitions(declared, topo.CPUDetails, reservedCPUs)
	degraded := make(map[string]string)
	for _, partition := range partitions {
		if reason := verifyThreadArity(topo.CPUDetails, partition); reason != "" {
			degraded[partition.Name] = reason
			logger.Info("CPU partition publishes no devices: the kernel contradicts its smt", "partition", partition.Name, "role", partition.Role, "cpus", partition.CPUs.String(), "reason", reason)
			continue
		}
		logger.Info("resolved CPU partition", "partition", partition.Name, "role", partition.Role, "cpus", partition.CPUs.String())
	}
	verifyOfflineAccounting(logger, topo.CPUDetails, presentCPUs, onlineCPUs, partitions)
	return partitions, degraded, nil
}

// validatePartitions refuses partitions this node cannot hold. The checks that
// need no topology ran when the configuration loaded.
func validatePartitions(details cpuinfo.CPUDetails, presentCPUs, onlineCPUs cpuset.CPUSet, declared []device.Partition) error {
	owner := make(map[cpuinfo.CoreLocation]device.Partition)
	for _, partition := range declared {
		if missing := partition.CPUs.Difference(presentCPUs); !missing.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s, which this node does not have: remove them from its cpuset",
				partition.Name, missing.String())
		}
		if offline := partition.CPUs.Difference(onlineCPUs); !offline.IsEmpty() {
			return fmt.Errorf("partition %q lists offline CPUs %s: bring them online, or remove them from its cpuset",
				partition.Name, offline.String())
		}
		if unmanaged := partition.CPUs.Difference(details.CPUs()); !unmanaged.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s with incomplete topology (the kernel reports no socket, core or NUMA node for them)",
				partition.Name, unmanaged.String())
		}
		for _, cpuID := range partition.CPUs.List() {
			loc, _ := details.CoreOf(cpuID)
			other, named := owner[loc]
			if named && other.Name != partition.Name {
				core := details.SiblingsOf(cpuID)
				return fmt.Errorf("partitions %q and %q split one core between them (%s in %q, %s in %q): put both threads in one partition",
					other.Name, partition.Name, cpuNoun(core.Intersection(other.CPUs)), other.Name, cpuNoun(core.Intersection(partition.CPUs)), partition.Name)
			}
			owner[loc] = partition
		}
	}
	for _, partition := range declared {
		if err := checkListedThreads(details, partition); err != nil {
			return err
		}
		// Under smt: false a core whose sibling is still online looks split; it
		// is the partition's smt that the kernel contradicts.
		if verifyThreadArity(details, partition) != "" {
			continue
		}
		if split := partition.CPUs.Difference(details.CompleteCores(partition.CPUs)); !split.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s without their sibling threads %s: list every thread of each core",
				partition.Name, split.String(), siblingsOutside(details, split, partition.CPUs).String())
		}
	}
	return nil
}

func cpuNoun(cpus cpuset.CPUSet) string {
	if cpus.Size() == 1 {
		return "CPU " + cpus.String()
	}
	return "CPUs " + cpus.String()
}

func siblingsOutside(details cpuinfo.CPUDetails, cpus, inside cpuset.CPUSet) cpuset.CPUSet {
	siblings := cpuset.New()
	for _, cpuID := range cpus.List() {
		siblings = siblings.Union(details.SiblingsOf(cpuID))
	}
	return siblings.Difference(inside)
}

// checkListedThreads refuses a partition that lists more threads of one core
// than its smt leaves online: taking the surplus offline would leave it naming
// an offline CPU, so no platform state makes the list valid.
func checkListedThreads(details cpuinfo.CPUDetails, partition device.Partition) error {
	if partition.ThreadsPerCore == 0 {
		return nil
	}
	for _, cpuID := range partition.CPUs.List() {
		listed := details.SiblingsOf(cpuID).Intersection(partition.CPUs)
		if listed.Size() <= partition.ThreadsPerCore {
			continue
		}
		smt, keep := strconv.Itoa(partition.ThreadsPerCore), fmt.Sprintf("the first %d threads", partition.ThreadsPerCore)
		if partition.ThreadsPerCore == 1 {
			smt, keep = "false", "the first thread"
		}
		return fmt.Errorf("partition %q lists CPUs %s of one core under `smt: %s`: list only %s of each core",
			partition.Name, listed.String(), smt, keep)
	}
	return nil
}

// verifyThreadArity compares the threads per core a partition's smt declares
// with the threads the kernel has online, and returns the contradiction, or the
// empty string where there is none. A partition that declares no smt accepts
// whatever the platform provides. The threads the partition does not list on
// its cores are the ones its smt asks the platform to take offline.
func verifyThreadArity(details cpuinfo.CPUDetails, partition device.Partition) string {
	if partition.ThreadsPerCore == 0 {
		return ""
	}
	surplus := cpuset.New()
	for _, cpuID := range partition.CPUs.List() {
		siblings := details.SiblingsOf(cpuID)
		if siblings.Size() <= partition.ThreadsPerCore {
			continue
		}
		surplus = surplus.Union(siblings.Difference(partition.CPUs))
	}
	if surplus.IsEmpty() {
		return ""
	}
	threads := "threads"
	if partition.ThreadsPerCore == 1 {
		threads = "thread"
	}
	return fmt.Sprintf("partition %q expects %d online %s per core, and CPUs %s of its cores are online too",
		partition.Name, partition.ThreadsPerCore, threads, surplus.String())
}

// verifyOfflineAccounting logs the offline CPUs no partition's smt accounts
// for: capacity nobody asked to lose, where the platform's configuration and
// the partitions disagree about the node. A partition's smt is checked by
// verifyThreadArity; this only reports.
func verifyOfflineAccounting(logger logr.Logger, details cpuinfo.CPUDetails, presentCPUs, onlineCPUs cpuset.CPUSet, partitions []device.Partition) {
	offline := presentCPUs.Difference(onlineCPUs)
	if offline.IsEmpty() {
		return
	}
	nativeThreadsPerCore := 0
	for _, cpuID := range onlineCPUs.List() {
		nativeThreadsPerCore = max(nativeThreadsPerCore, details.SiblingsOf(cpuID).Size())
	}
	if nativeThreadsPerCore <= 1 {
		// An offline thread is in no core the topology shows, so a core the
		// platform halved looks like a core that never had a sibling.
		logger.Info("offline CPUs cannot be accounted for: every online core has one thread", "offline", offline.String())
		return
	}
	accounted := 0
	for _, partition := range partitions {
		if partition.ThreadsPerCore == 0 || partition.ThreadsPerCore >= nativeThreadsPerCore {
			continue
		}
		cores := details.CompleteCores(partition.CPUs).Size() / partition.ThreadsPerCore
		accounted += cores * (nativeThreadsPerCore - partition.ThreadsPerCore)
	}
	if offline.Size() == accounted {
		return
	}
	logger.Info("offline CPUs the partitions' smt does not account for: they are lost capacity, and the platform's configuration and the partitions disagree about this node",
		"offline", offline.String(), "offlineCount", offline.Size(), "accountedFor", accounted, "nativeThreadsPerCore", nativeThreadsPerCore)
}

// partitionDegradedCondition reports the partitions whose smt the kernel
// contradicts, decided at start.
func (cp *CPUDriver) partitionDegradedCondition() nodeConditionSource {
	return nodeConditionSource{
		conditionType: conditionPartitionDegraded,
		evaluate: func() nodeConditionValue {
			if len(cp.degradedPartitions) == 0 {
				return partitionsVerified
			}
			return nodeConditionValue{
				status:  corev1.ConditionTrue,
				reason:  "SiblingThreadsOnline",
				message: partitionDegradedMessage(cp.degradedPartitions),
			}
		},
		stoppedMessage: "driver stopped; partitions not checked",
	}
}

// partitionDegradedMessage is CPUPartitionDegraded's message for the degraded
// partitions, in name order.
func partitionDegradedMessage(degraded map[string]string) string {
	names := make([]string, 0, len(degraded))
	for name := range degraded {
		names = append(names, name)
	}
	slices.Sort(names)
	reasons := make([]string, 0, len(names))
	for _, name := range names {
		reasons = append(reasons, degraded[name])
	}
	return "partitions publish no devices while the kernel contradicts their smt: " + strings.Join(reasons, "; ") +
		"; take those CPUs offline at boot, then restart the driver"
}

// defaultPartitionCPUs is the CPUs of the entry device.DefaultPartitionName,
// where containers without claims run; a named partition of role default
// takes claims on its devices only.
func defaultPartitionCPUs(partitions []device.Partition) cpuset.CPUSet {
	for _, partition := range partitions {
		if partition.Name == device.DefaultPartitionName {
			return partition.CPUs
		}
	}
	return cpuset.New()
}

// unclaimablePartitionCPUs is every CPU of a partition that keeps its CPUs from
// every workload.
func unclaimablePartitionCPUs(partitions []device.Partition) cpuset.CPUSet {
	unclaimable := cpuset.New()
	for _, partition := range partitions {
		if partition.IsReserved() {
			unclaimable = unclaimable.Union(partition.CPUs)
		}
	}
	return unclaimable
}
