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

	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/discovery"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"
)

var _ = ginkgo.Describe("Whole physical cores", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		targetNodeCPUInfo discovery.DRACPUInfo
		dracpuTesterImage string
		cfgValues         driverConfigValues
		// siblingOf maps a CPU to the other threads of its physical core.
		siblingOf map[int]cpuset.CPUSet
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		infraFxt := rootFxt.WithPrefix("infra-cores")
		gomega.Expect(infraFxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(infraFxt.Teardown)

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		if !cfgValues.FullPhysicalCPUsOnly {
			ginkgo.Skip("fullPhysicalCPUsOnly is not enabled in the driver configuration; set DRACPU_E2E_FULL_PCPUS_ONLY=true when creating the cluster")
		}

		infoPod := discovery.MakePod(infraFxt.Namespace.Name, dracpuTesterImage)
		infoPod = e2epod.PinToNode(infoPod, targetNode.Name)
		infoPod, err = e2epod.RunToCompletion(ctx, infraFxt.K8SClientset, infoPod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		data, err := e2epod.GetLogs(ctx, infraFxt.K8SClientset, infoPod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(unmarshalLatestReport(data, &targetNodeCPUInfo)).To(gomega.Succeed())

		// Group the node's CPUs by the physical core they belong to. Core IDs are
		// only unique within a socket, so the socket has to be part of the key.
		type coreKey struct{ socketID, coreID int }
		byCore := map[coreKey][]int{}
		for _, cpu := range targetNodeCPUInfo.CPUs {
			key := coreKey{socketID: cpu.SocketID, coreID: cpu.CoreID}
			byCore[key] = append(byCore[key], cpu.CpuID)
		}
		siblingOf = map[int]cpuset.CPUSet{}
		threaded := 0
		for _, cpus := range byCore {
			if len(cpus) > 1 {
				threaded++
			}
			core := cpuset.New(cpus...)
			for _, cpu := range cpus {
				siblingOf[cpu] = core
			}
		}
		rootFxt.Log.Info("core topology", "cores", len(byCore), "coresWithSiblings", threaded)

		// With SMT off every core is one thread, so whole-core allocation is
		// satisfied by any cpuset and there is nothing to observe.
		if threaded == 0 {
			ginkgo.Skip("whole-core allocation only has observable effects where SMT is enabled")
		}
	})

	ginkgo.It("should never split a physical core between a claim and anything else", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("whole-cores")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		// One CPU is deliberately not a whole core. The scheduler rounds the
		// request up to the core size through the capacity requestPolicy the
		// driver publishes, which is where this differs from the kubelet CPU
		// Manager: that rejects the pod instead.
		ginkgo.By("requesting a single CPU where a core has two threads")
		pod, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 1, "cpu-claim-one-thread")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		fxt.Log.Info("allocation for a one-CPU request", "cpus", alloc.CPUAssigned.String())

		ginkgo.By("verifying every core it touches is entirely its own")
		requireWholeCores(alloc.CPUAssigned, siblingOf)

		ginkgo.By("verifying the kernel affinity agrees with the cgroup cpuset")
		gomega.Expect(alloc.CPUAffinity.Equals(alloc.CPUAssigned)).To(gomega.BeTrue(),
			"affinity %s does not match cpuset %s", alloc.CPUAffinity.String(), alloc.CPUAssigned.String())
	})

	ginkgo.It("should keep a shared container off an exclusive claim's cores", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("whole-cores-shared")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("placing an exclusive claim")
		claimed, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-with-shared")
		exclusive := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, claimed).CPUAssigned.Difference(cfgValues.effectiveFor(targetNode).staticPool())
		requireWholeCores(exclusive, siblingOf)

		ginkgo.By("placing a container with no claim beside it")
		sharedPod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)
		shared := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod).CPUAssigned

		// The point of whole-core allocation: the shared pool is whole cores too,
		// so no ordinary container contends for a hyperthread of an exclusive
		// claim's core.
		ginkgo.By("verifying it holds no thread of an exclusive core")
		gomega.Expect(shared).To(cpusetmatchers.HaveNoOverlapWith(exclusive),
			"shared container on %s overlaps the exclusive claim on %s", shared.String(), exclusive.String())
		for _, cpu := range exclusive.List() {
			gomega.Expect(shared).To(cpusetmatchers.HaveNoOverlapWith(siblingOf[cpu]),
				"shared container on %s holds a sibling of exclusive CPU %d (core %s)",
				shared.String(), cpu, siblingOf[cpu].String())
		}
	})
})

// requireWholeCores fails unless every physical core the cpuset touches is
// included in full.
func requireWholeCores(cpus cpuset.CPUSet, siblingOf map[int]cpuset.CPUSet) {
	ginkgo.GinkgoHelper()
	for _, cpu := range cpus.List() {
		core, ok := siblingOf[cpu]
		gomega.Expect(ok).To(gomega.BeTrue(), "CPU %d is not in the discovered topology", cpu)
		gomega.Expect(core.IsSubsetOf(cpus)).To(gomega.BeTrue(),
			"cpuset %s holds only part of core %s", cpus.String(), core.String())
	}
}
