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
	"fmt"
	"os"
	"time"

	e2eclaim "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/claim"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/discovery"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/cpuset"
)

var _ = ginkgo.Describe("Whole physical cores", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, ginkgo.Label("whole-cores"), func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		dracpuTesterImage string
		cfgValues         driverConfigValues
		// coreOf maps each CPU of the target node to the CPUs of its core.
		coreOf map[int]cpuset.CPUSet
		// step is the threads per core of the target node's devices.
		step int
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		if !cfgValues.FullPhysicalCPUsOnly {
			ginkgo.Skip("fullPhysicalCPUsOnly is not set in the driver configuration; set DRACPU_E2E_FULL_PCPUS_ONLY=true when creating the cluster")
		}

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name)

		infraFxt := rootFxt.WithPrefix("infra-cores")
		gomega.Expect(infraFxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(infraFxt.Teardown)
		infoPod := discovery.MakePod(infraFxt.Namespace.Name, dracpuTesterImage)
		infoPod = e2epod.PinToNode(infoPod, targetNode.Name)
		infoPod, err = e2epod.RunToCompletion(ctx, infraFxt.K8SClientset, infoPod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot run the discovery pod")
		data, err := e2epod.GetLogs(ctx, infraFxt.K8SClientset, infoPod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get the discovery pod's logs")
		var cpuInfo discovery.DRACPUInfo
		gomega.Expect(unmarshalLatestReport(data, &cpuInfo)).To(gomega.Succeed())
		coreOf = coresOf(cpuInfo)

		step, err = e2eclaim.AllocationStep(ctx, rootFxt.K8SClientset, driverName, targetNode.Name)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		rootFxt.Log.Info("whole-core allocation", "nodeName", targetNode.Name, "threadsPerCore", step)
		// A device of one-thread cores, or of cores that differ, has no request
		// policy, and any amount of it is whole cores.
		if step < 2 {
			ginkgo.Skip(fmt.Sprintf("node %s publishes no device whose cores all have two logical CPUs", targetNode.Name))
		}
	})

	ginkgo.It("should give an exclusive claim whole cores", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("whole-cores")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("running a pod on a claim for two cores")
		pod, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2*step, "cpu-claim-two-cores")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		fxt.Log.Info("exclusive allocation", "pod", e2epod.Identify(pod), "cpus", alloc.CPUAssigned.String())

		ginkgo.By("checking every core the claim touches is wholly its own")
		gomega.Expect(alloc.CPUAssigned).To(cpusetmatchers.HaveSize(2 * step))
		requireWholeCores(alloc.CPUAssigned, coreOf)
		gomega.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(alloc.CPUAssigned), "the affinity differs from the cgroup cpuset")
		gomega.Expect(alloc.CPUReported).To(cpusetmatchers.Equal(alloc.CPUAssigned), "the reported cpuset differs from the cgroup cpuset")
	})

	ginkgo.It("should keep a container without a claim off an exclusive claim's cores", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("whole-cores-shared")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("running a pod on a claim for one core")
		claimed, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, step, "cpu-claim-one-core")
		exclusive := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, claimed).CPUAssigned
		requireWholeCores(exclusive, coreOf)

		ginkgo.By("running a pod without a claim beside it")
		sharedPod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)
		shared := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod).CPUAssigned
		fxt.Log.Info("allocations", "exclusive", exclusive.String(), "shared", shared.String())

		ginkgo.By("checking the pod without a claim holds no thread of the claim's cores")
		exclusiveCores := cpuset.New()
		for _, cpu := range exclusive.List() {
			exclusiveCores = exclusiveCores.Union(coreOf[cpu])
		}
		gomega.Expect(shared.IsEmpty()).To(gomega.BeFalse(), "the pod without a claim runs on no CPU")
		gomega.Expect(shared).To(cpusetmatchers.HaveNoOverlapWith(exclusiveCores),
			"the pod without a claim runs on %s, which shares a core with the claim's %s", shared.String(), exclusive.String())
	})

	ginkgo.It("should refuse to prepare an amount that whole cores do not make up", ginkgo.Label("negative"), func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("whole-cores-refused")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		// The device's request policy has the scheduler round the amount up to a
		// core, and the driver refuses what the claim did not ask for. A device
		// whose cores differ has no policy, and would take the amount.
		written := step - 1
		ginkgo.By(fmt.Sprintf("creating a pod on a claim for %d CPU, less than a core", written))
		spec := e2eclaim.WithSelector(makeResourceClaimSpec(written, true),
			fmt.Sprintf(`device.attributes["dra.cpu"].threadsPerCore == %d`, step))
		template := &resourcev1.ResourceClaimTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "cpu-claim-part-of-a-core"},
			Spec:       resourcev1.ResourceClaimTemplateSpec{Spec: spec},
		}
		_, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, template, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		pod := makeTesterPodWithExclusiveCPUClaimWithoutCPURequest(fxt.Namespace.Name, dracpuTesterImage, template.Name, targetNode.Name)
		pod, err = fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("waiting for the kubelet to report the driver's refusal")
		refusal := fmt.Sprintf("request \"request-cpus\": amount %d on exclusive request (class %s); ask for whole cores of %d threads (`fullPhysicalCPUsOnly: true`)", written, driverName, step)
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

		ginkgo.By("checking the pod does not run")
		current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(current.Status.Phase).To(gomega.Equal(v1.PodPending), "a pod whose claim the driver refused to prepare runs")
	})
})

// coresOf maps each CPU of info to the CPUs of its core, which the discovery
// report names by sibling, as upstream's core identity pairs them.
func coresOf(info discovery.DRACPUInfo) map[int]cpuset.CPUSet {
	cores := make(map[int]cpuset.CPUSet, len(info.CPUs))
	for _, cpu := range info.CPUs {
		core := cpuset.New(cpu.CpuID)
		if cpu.SiblingCPUID >= 0 {
			core = core.Union(cpuset.New(cpu.SiblingCPUID))
		}
		cores[cpu.CpuID] = core
	}
	return cores
}

// requireWholeCores fails unless every core cpus touches is wholly in cpus.
func requireWholeCores(cpus cpuset.CPUSet, coreOf map[int]cpuset.CPUSet) {
	ginkgo.GinkgoHelper()
	for _, cpu := range cpus.List() {
		core, ok := coreOf[cpu]
		gomega.Expect(ok).To(gomega.BeTrue(), "CPU %d is not in the discovered topology", cpu)
		gomega.Expect(core.IsSubsetOf(cpus)).To(gomega.BeTrue(), "cpuset %s holds only part of core %s", cpus.String(), core.String())
	}
}
