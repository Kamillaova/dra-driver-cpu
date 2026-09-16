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

package e2e

import (
	"context"
	"os"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/cpuset"
)

// A pool is reached by claiming it, so these specs need a node whose cores
// declare a partition of role shared. Where none is configured they are
// reported as not run rather than as passed.
var _ = ginkgo.Describe("Claimed CPU pool", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		dracpuTesterImage string
		cfgValues         driverConfigValues
		poolName          string
		poolCPUs          cpuset.CPUSet
		poolDevices       []resourcev1.Device
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name)

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset).effectiveFor(targetNode)
		pool, ok := cfgValues.poolPartition()
		if !ok {
			ginkgo.Skip("no CPU pool is configured in the driver; set DRACPU_E2E_POOL_PARTITION_CPUS when creating the cluster")
		}
		poolName = pool.Name
		poolCPUs, err = cpuset.Parse(pool.CPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse the cpus of partition %q", poolName)

		poolDevices = devicesOfPartition(ctx, rootFxt, targetNode.Name, poolName)
		gomega.Expect(poolDevices).ToNot(gomega.BeEmpty(), "partition %s publishes no device on node %s", poolName, targetNode.Name)
	})

	ginkgo.It("should publish the pool as shareable devices with a fractional policy", func() {
		published := 0
		for _, dev := range poolDevices {
			gomega.Expect(dev.AllowMultipleAllocations).ToNot(gomega.BeNil())
			gomega.Expect(*dev.AllowMultipleAllocations).To(gomega.BeTrue(),
				"device %s is not shareable, so only one claim could ever hold the pool", dev.Name)
			gomega.Expect(dev.NodeAllocatableResources).To(gomega.BeEmpty(),
				"device %s maps pool CPUs to node allocatable, which counts them twice", dev.Name)
			gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeRole,
				resourcev1.DeviceAttribute{StringValue: new(device.PARTITION_ROLE_SHARED)}))

			capacity := dev.Capacity[device.CPUResourceQualifiedName]
			gomega.Expect(capacity.RequestPolicy).ToNot(gomega.BeNil(),
				"device %s has no request policy, so a size-less request would take the whole pool", dev.Name)
			gomega.Expect(capacity.RequestPolicy.Default.Cmp(resource.MustParse("100m"))).To(gomega.Equal(0))
			gomega.Expect(capacity.RequestPolicy.ValidRange.Min.Cmp(resource.MustParse("100m"))).To(gomega.Equal(0))

			numCPUs := dev.Attributes[device.AttributeNumCPUs]
			gomega.Expect(numCPUs.IntValue).ToNot(gomega.BeNil(), "device %s publishes no CPU count", dev.Name)
			published += int(*numCPUs.IntValue)
		}
		gomega.Expect(published).To(gomega.Equal(poolCPUs.Size()),
			"the published devices hold %d CPUs, the partition names %d", published, poolCPUs.Size())
	})

	ginkgo.It("should give a claim its exclusive CPUs plus the whole pool", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("pool-claim")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		pod, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			poolClaimSpec(poolName, 2), "cpu-claim-pool")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		exclusive := alloc.CPUAssigned.Difference(poolCPUs)
		gomega.Expect(exclusive).To(cpusetmatchers.HaveSize(2), "the claim did not get two CPUs of its own")

		ginkgo.By("verifying the pool CPUs are in the container's cpuset and the kernel agrees")
		inPool := alloc.CPUAssigned.Intersection(poolCPUs)
		gomega.Expect(inPool).ToNot(cpusetmatchers.HaveSize(0), "the claimed pool did not reach the container")
		gomega.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(alloc.CPUAssigned))

		ginkgo.By("verifying the pool request names its CPUs in its own metadata file")
		var seen bool
		for _, entry := range alloc.Metadata {
			if entry.Request != "helpers" {
				continue
			}
			seen = true
			gomega.Expect(entry.Partition).To(gomega.Equal(poolName))
			gomega.Expect(entry.Role).To(gomega.Equal(device.PARTITION_ROLE_SHARED))
			cpus, err := cpuset.Parse(entry.CPUs)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse the metadata cpuset %q", entry.CPUs)
			gomega.Expect(cpus).To(cpusetmatchers.Equal(inPool),
				"the metadata file and the container's cpuset disagree about the pool")
		}
		gomega.Expect(seen).To(gomega.BeTrue(), "no metadata file was mounted for the helpers request: %+v", alloc.Metadata)
	})

	ginkgo.It("should let two claims hold the same pool at once", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("pool-shared")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		first, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			poolClaimSpec(poolName, 2), "cpu-claim-pool-first")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		second, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			poolClaimSpec(poolName, 2), "cpu-claim-pool-second")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		firstAlloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, first)
		secondAlloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, second)

		gomega.Expect(firstAlloc.CPUAssigned.Intersection(poolCPUs)).To(
			cpusetmatchers.Equal(secondAlloc.CPUAssigned.Intersection(poolCPUs)),
			"two claims of the same pool were given different CPUs")
		gomega.Expect(firstAlloc.CPUAssigned.Difference(poolCPUs)).To(
			cpusetmatchers.HaveNoOverlapWith(secondAlloc.CPUAssigned.Difference(poolCPUs)),
			"the exclusive halves of two claims overlap")
	})

	ginkgo.It("should keep a container holding no claim off the pool", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("pool-claimless")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		pod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)
		gomega.Eventually(observeAssignedCPUs(ctx, fxt, pod)).WithTimeout(1*time.Minute).WithPolling(5*time.Second).
			Should(cpusetmatchers.HaveNoOverlapWith(poolCPUs),
				"the claimless pod %s runs on the pool, which is reached by claiming it", e2epod.Identify(pod))
	})
})

// poolClaimSpec is the canonical VM shape: one claim with an exclusive request
// and a request for a share of the node's pool. Two requests of one claim
// rather than two claims, since a container holding two claims is pinned to
// their union and cannot tell which CPUs came from which.
func poolClaimSpec(poolName string, cpus int) resourcev1.ResourceClaimSpec {
	spec := makeResourceClaimSpec(cpus, true)
	spec.Devices.Requests = append(spec.Devices.Requests, resourcev1.DeviceRequest{
		Name: "helpers",
		Exactly: &resourcev1.ExactDeviceRequest{
			DeviceClassName: driverName + "-" + poolName,
			AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
			Count:           1,
			Capacity: &resourcev1.CapacityRequirements{
				Requests: map[resourcev1.QualifiedName]resource.Quantity{
					device.CPUResourceQualifiedName: resource.MustParse("500m"),
				},
			},
			Tolerations: []resourcev1.DeviceToleration{
				{
					Key:      device.PartitionTaintKey,
					Operator: resourcev1.DeviceTolerationOpEqual,
					Value:    poolName,
					Effect:   resourcev1.DeviceTaintEffectNoSchedule,
				},
			},
		},
	})
	return spec
}
