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
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/discovery"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/cpuset"
)

// A single-thread partition needs a node whose SMT siblings an operator took
// offline beforehand. The suite never changes a machine, so where no such node
// exists these specs are reported as not run rather than as passed.
var _ = ginkgo.Describe("Single-thread CPU partition", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		dracpuTesterImage string
		cfgValues         driverConfigValues
		partitionName     string
		partitionCPUs     cpuset.CPUSet
		partitionDevices  []resourcev1.Device
		nodeCPUInfo       discovery.DRACPUInfo
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
		partition, ok := cfgValues.singleThreadPartition()
		if !ok {
			ginkgo.Skip("no single-thread CPU partition is configured in the driver; set DRACPU_E2E_NOSMT_PARTITION_CPUS when creating the cluster, on a node whose SMT siblings are already offline")
		}
		partitionName = partition.Name
		partitionCPUs, err = cpuset.Parse(partition.CPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse the cpus of partition %q", partitionName)

		partitionDevices = devicesOfPartition(ctx, rootFxt, targetNode.Name, partitionName)
		if len(partitionDevices) == 0 {
			ginkgo.Skip("partition " + partitionName + " publishes no device on node " + targetNode.Name + ": the driver withheld it, which is what a node whose siblings are still online looks like")
		}

		infraFxt := rootFxt.WithPrefix("infra-partition")
		gomega.Expect(infraFxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(infraFxt.Teardown)
		nodeCPUInfo = discoverNodeCPUInfo(ctx, infraFxt, targetNode.Name, dracpuTesterImage)
	})

	ginkgo.It("should publish tainted single-thread devices for the partition", func() {
		for _, dev := range partitionDevices {
			gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeThreadsPerCore,
				resourcev1.DeviceAttribute{IntValue: new(int64(1))}), "device %s does not report one thread per core", dev.Name)
			gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeSMTEnabled,
				resourcev1.DeviceAttribute{BoolValue: new(false)}), "device %s reports SMT enabled", dev.Name)
			gomega.Expect(dev.Taints).To(gomega.ContainElement(resourcev1.DeviceTaint{
				Key:    device.PartitionTaintKey,
				Value:  partitionName,
				Effect: resourcev1.DeviceTaintEffectNoSchedule,
			}), "device %s carries no partition taint, so any class could take it", dev.Name)
			gomega.Expect(dev.Capacity[device.CPUResourceQualifiedName].RequestPolicy).To(gomega.BeNil(),
				"device %s publishes a whole-core step, but one thread is already a whole core here", dev.Name)
		}
	})

	ginkgo.It("should give a claim on the partition whole cores of its own", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("partition-claim")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		pod, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			partitionClaimSpec(partitionName, 2), "cpu-claim-partition")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
		gomega.Expect(assigned.IsSubsetOf(partitionCPUs)).To(gomega.BeTrue(),
			"the claim took %s, which is not inside partition %s (%s)", assigned.String(), partitionName, partitionCPUs.String())

		for _, cpuID := range assigned.List() {
			gomega.Expect(siblingsOf(nodeCPUInfo, cpuID)).To(cpusetmatchers.Equal(cpuset.New(cpuID)),
				"CPU %d still has an online sibling, so this node was not prepared as the partition declares", cpuID)
		}
	})

	ginkgo.It("should keep a claim that names no partition off it", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("partition-default")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		pod, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-default-partition")

		assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
		gomega.Expect(assigned).To(cpusetmatchers.HaveNoOverlapWith(partitionCPUs),
			"a claim of the default class landed on partition %s", partitionName)
	})
})

// devicesOfPartition returns the devices the node publishes for one partition.
func devicesOfPartition(ctx context.Context, fxt *fixture.Fixture, nodeName, partitionName string) []resourcev1.Device {
	ginkgo.GinkgoHelper()
	sliceList, err := fxt.K8SClientset.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{
		FieldSelector: "spec.driver=" + driverName,
	})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list ResourceSlices")

	var devices []resourcev1.Device
	for _, slice := range sliceList.Items {
		if slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
			continue
		}
		for _, dev := range slice.Spec.Devices {
			attr, ok := dev.Attributes[device.AttributePartition]
			if !ok || attr.StringValue == nil || *attr.StringValue != partitionName {
				continue
			}
			devices = append(devices, dev)
		}
	}
	return devices
}

// partitionClaimSpec is what a workload asks for when it wants a named
// partition: that partition's own device class, and the toleration for the
// taint every named partition carries.
func partitionClaimSpec(partitionName string, cpus int) resourcev1.ResourceClaimSpec {
	spec := makeResourceClaimSpec(cpus, true)
	spec.Devices.Requests[0].Exactly.DeviceClassName = driverName + "-" + partitionName
	spec.Devices.Requests[0].Exactly.Tolerations = []resourcev1.DeviceToleration{
		{
			Key:      device.PartitionTaintKey,
			Operator: resourcev1.DeviceTolerationOpEqual,
			Value:    partitionName,
			Effect:   resourcev1.DeviceTaintEffectNoSchedule,
		},
	}
	return spec
}

// siblingsOf is what the node itself reports as sharing a physical core with
// cpuID, which on a prepared node is that CPU alone.
func siblingsOf(info discovery.DRACPUInfo, cpuID int) cpuset.CPUSet {
	for _, cpu := range info.CPUs {
		if cpu.CpuID == cpuID {
			return cpu.SiblingCPUSet
		}
	}
	return cpuset.New()
}
