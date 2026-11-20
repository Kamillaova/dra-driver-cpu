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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	e2eclient "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/client"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// allowingProfile is the lane's profile whose default entry sets
// allowExclusive, which ci-kind-setup gives worker2.
const allowingProfile = "e2e-wide"

// A node whose default partition sets allowExclusive gives a request with
// `cpuConfig.exclusive: true` CPUs of its own out of the shared device, and the
// containers without claims and the share holders leave them until the claim
// is unprepared; a node whose default partition does not refuses the claim at
// Prepare. Where no node runs the lane's allowing profile these specs skip.
var _ = ginkgo.Describe("Exclusive requests on a default partition", ginkgo.Label("partitions"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		restConfig        *rest.Config
		dracpuTesterImage string
		cfg               driverConfigValues
		allowing          *v1.Node
		refusing          *v1.Node
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		restConfig, err = e2eclient.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create Kubernetes config")

		nodes, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel + "=" + allowingProfile})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if len(nodes.Items) == 0 {
			ginkgo.Skip("no node runs the profile " + allowingProfile + "; create the cluster with DRACPU_E2E_LANE=partitions")
		}
		allowing = &nodes.Items[0]
		cfg = getDriverConfig(ctx, rootFxt.K8SClientset)
		gomega.Expect(defaultEntry(cfg.effectiveFor(allowing)).AllowExclusive).To(gomega.BeTrue(),
			"node %s runs %s, whose default entry does not set allowExclusive", allowing.Name, allowingProfile)

		workers, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/worker"})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		for i := range workers.Items {
			node := &workers.Items[i]
			if values := cfg.effectiveFor(node); values.partitionsUsed() && !defaultEntry(values).AllowExclusive {
				refusing = node
				break
			}
		}
		if refusing != nil {
			rootFxt.Log.Info("using nodes", "allowing", allowing.Name, "refusing", refusing.Name)
		}
	})

	ginkgo.It("should give a flagged request CPUs of its own and move the other containers off them until it is deleted", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("excl-default")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		numCPUs, threads := defaultSharedDevice(ctx, rootFxt, allowing.Name)
		amount := 1
		if cfg.FullPhysicalCPUsOnly && threads > 1 {
			amount = threads
		}
		gomega.Expect(numCPUs).To(gomega.BeNumerically(">", amount),
			"node %s's shared device holds %d CPUs, too few to leave one shared beside %d exclusive CPUs", allowing.Name, numCPUs, amount)

		// With one device of the default partition and no claim prepared, the
		// containers without claims run on the device's CPUs.
		plain := mustCreateBestEffortPod(ctx, fxt, allowing.Name, dracpuTesterImage)
		shared := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, plain).CPUAssigned
		gomega.Expect(shared).To(cpusetmatchers.HaveSize(numCPUs), "pod %s does not run on the shared device's %d CPUs", e2epod.Identify(plain), numCPUs)
		share := makeResourceClaimSpec(1, true)
		share.Devices.Requests[0].Exactly.Capacity = nil
		holder := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, allowing.Name, share, "excl-default-share")
		gomega.Eventually(observeAssignedCPUs(ctx, fxt, holder)).WithTimeout(time.Minute).WithPolling(5*time.Second).Should(cpusetmatchers.Equal(shared),
			"the share holder %s does not run on the shared device's CPUs", e2epod.Identify(holder))

		fixture.By("creating a pod whose claim asks for %d exclusive CPUs on dra.cpu", amount)
		flagged := makeResourceClaimSpec(amount, true)
		withExclusive(&flagged, true)
		exclusivePod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, allowing.Name, flagged, "excl-default-flagged")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, exclusivePod)
		exclusive := alloc.CPUAssigned
		gomega.Expect(exclusive).To(cpusetmatchers.HaveSize(amount))
		gomega.Expect(exclusive.IsSubsetOf(shared)).To(gomega.BeTrue(), "the exclusive CPUs %s are not the shared device's %s", exclusive.String(), shared.String())
		gomega.Expect(alloc.CPUReported).To(cpusetmatchers.Equal(exclusive), "DRA_EXCLUSIVE_ASSIGNED_CPUSET does not name the exclusive CPUs")
		gomega.Expect(alloc.CPUMax).To(gomega.Equal("max 100000"), "an exclusive request runs under a quota")
		devicesByRequest := readMetadataDevices(ctx, restConfig, fxt, exclusivePod)
		gomega.Expect(devicesByRequest).To(gomega.HaveKey("request-cpus"))
		for _, dev := range devicesByRequest["request-cpus"] {
			expectStringAttrValue(dev.Attributes, "dra.cpu/cpuset", exclusive.String())
			expectStringAttrValue(dev.Attributes, "dra.cpu/cpuLimit", "max")
		}

		fixture.By("checking the container without a claim and the share holder left the exclusive CPUs")
		for _, pod := range []*v1.Pod{plain, holder} {
			gomega.Eventually(observeAssignedCPUs(ctx, fxt, pod)).WithTimeout(time.Minute).WithPolling(5*time.Second).Should(cpusetmatchers.Equal(shared.Difference(exclusive)),
				"pod %s did not leave the exclusive CPUs %s", e2epod.Identify(pod), exclusive.String())
		}

		fixture.By("deleting the pod with the flagged claim, which unprepares it")
		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, exclusivePod)).To(gomega.Succeed(), "cannot delete pod %s", e2epod.Identify(exclusivePod))
		if !cfg.AssumeUnsolicitedUpdatesSafe {
			fixture.By("creating a claim's first container, which carries the give-back without assumeUnsolicitedUpdatesSafe")
			createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, allowing.Name, share, "excl-default-trigger")
		}
		for _, pod := range []*v1.Pod{plain, holder} {
			gomega.Eventually(observeAssignedCPUs(ctx, fxt, pod)).WithTimeout(time.Minute).WithPolling(5*time.Second).Should(cpusetmatchers.Equal(shared),
				"pod %s did not get the exclusive CPUs back", e2epod.Identify(pod))
		}
	})

	ginkgo.It("should refuse a flagged request on a node whose default partition does not allow it", ginkgo.Label("negative"), func(ctx context.Context) {
		if refusing == nil {
			ginkgo.Skip("no worker runs a profile whose default partition leaves allowExclusive false")
		}
		fxt := rootFxt.WithPrefix("excl-default-refused")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		spec := makeResourceClaimSpec(1, true)
		withExclusive(&spec, true)
		template := &resourcev1.ResourceClaimTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "excl-default-refused"},
			Spec:       resourcev1.ResourceClaimTemplateSpec{Spec: spec},
		}
		_, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, template, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		pod := makeTesterPodWithExclusiveCPUClaimWithoutCPURequest(fxt.Namespace.Name, dracpuTesterImage, template.Name, refusing.Name)
		pod, err = fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("waiting for the kubelet to report the driver's refusal")
		refusal := fmt.Sprintf("request \"request-cpus\": partition \"default\" on node %q does not allow exclusive CPUs (`allowExclusive: false`)", refusing.Name)
		gomega.Eventually(func(g gomega.Gomega) {
			events, err := fxt.K8SClientset.CoreV1().Events(pod.Namespace).List(ctx, metav1.ListOptions{
				FieldSelector: "involvedObject.name=" + pod.Name,
			})
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(events.Items).To(gomega.ContainElement(gomega.And(
				gomega.HaveField("Reason", "FailedPrepareDynamicResources"),
				gomega.HaveField("Message", gomega.ContainSubstring(refusal)),
			)), "no event of pod %s carries the refusal %q", e2epod.Identify(pod), refusal)
		}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())

		current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(current.Status.Phase).To(gomega.Equal(v1.PodPending), "a pod whose claim the driver refused to prepare runs")
	})
})

// defaultEntry returns the configuration's entry device.DefaultPartitionName.
func defaultEntry(values driverConfigValues) driverconfig.CPUPartition {
	ginkgo.GinkgoHelper()
	idx := slices.IndexFunc(values.CPUPartitions, func(p driverconfig.CPUPartition) bool { return p.Name == device.DefaultPartitionName })
	gomega.Expect(idx).ToNot(gomega.Equal(-1), "the configuration declares no partition %q", device.DefaultPartitionName)
	return values.CPUPartitions[idx]
}

// defaultSharedDevice returns the CPU count and threads per core of the
// node's one device of the default partition.
func defaultSharedDevice(ctx context.Context, fxt *fixture.Fixture, nodeName string) (int, int) {
	ginkgo.GinkgoHelper()
	var found []resourcev1.Device
	for _, dev := range nodeDevices(ctx, gomega.Default, fxt.K8SClientset, nodeName) {
		if partition := dev.Attributes[device.AttributePartition].StringValue; partition != nil && *partition == device.DefaultPartitionName {
			found = append(found, dev)
		}
	}
	gomega.Expect(found).To(gomega.HaveLen(1), "node %s does not publish one device of partition %q", nodeName, device.DefaultPartitionName)
	numCPUs := found[0].Attributes[device.AttributeNumCPUs].IntValue
	gomega.Expect(numCPUs).ToNot(gomega.BeNil(), "device %s publishes no %s", found[0].Name, device.AttributeNumCPUs)
	threads := found[0].Attributes[device.AttributeThreadsPerCore].IntValue
	gomega.Expect(threads).ToNot(gomega.BeNil(), "device %s publishes no %s", found[0].Name, device.AttributeThreadsPerCore)
	return int(*numCPUs), int(*threads)
}

// withExclusive gives the spec's first request a cpuConfig.exclusive.
func withExclusive(spec *resourcev1.ResourceClaimSpec, exclusive bool) {
	ginkgo.GinkgoHelper()
	raw, err := json.Marshal(v1alpha1.OpaqueConfig{APIVersion: v1alpha1.APIVersion, CPUConfig: v1alpha1.CPUConfig{Exclusive: new(exclusive)}})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	spec.Devices.Config = append(spec.Devices.Config, resourcev1.DeviceClaimConfiguration{
		Requests: []string{spec.Devices.Requests[0].Name},
		DeviceConfiguration: resourcev1.DeviceConfiguration{Opaque: &resourcev1.OpaqueDeviceConfiguration{
			Driver: driverName, Parameters: runtime.RawExtension{Raw: raw},
		}},
	})
}
