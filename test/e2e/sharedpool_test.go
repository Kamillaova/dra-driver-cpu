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

var _ = ginkgo.Describe("Static shared pool", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		dracpuTesterImage string
		cfgValues         driverConfigValues
		staticPool        cpuset.CPUSet
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

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		staticPool = cfgValues.effectiveFor(targetNode).staticPool()
		if staticPool.IsEmpty() {
			ginkgo.Skip("no static shared pool is configured in the driver; set DRACPU_E2E_SHARED_POOL_CPUS when creating the cluster")
		}

		infraFxt := rootFxt.WithPrefix("infra-pool")
		gomega.Expect(infraFxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(infraFxt.Teardown)
		nodeCPUInfo = discoverNodeCPUInfo(ctx, infraFxt, targetNode.Name, dracpuTesterImage)
	})

	ginkgo.It("should pin claimless containers to the pool and never re-pin them", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("pool-shared")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("creating a claimless best-effort pod")
		sharedPod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)
		verifySharedPoolMatches(ctx, fxt, sharedPod, staticPool)

		ginkgo.By("verifying it carries no shared-pool contract of its own")
		gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod).SharedEnvCPUs).To(gomega.BeEmpty(),
			"a claimless container must not see DRA_SHARED_CPUS")

		ginkgo.By("verifying a claim arriving does not move it")
		claimed, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-pool-churn")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod).CPUAssigned).To(cpusetmatchers.Equal(staticPool))
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("verifying the claim leaving does not move it either")
		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, claimed)).To(gomega.Succeed())
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod).CPUAssigned).To(cpusetmatchers.Equal(staticPool))
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should hand a guaranteed container its claims plus the NUMA-local pool", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("pool-guaranteed")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("creating a pod with an exclusive claim")
		pod, claimUID := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-pool-guaranteed")

		ginkgo.By("reading where the driver placed the claim")
		var claimCPUs cpuset.CPUSet
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			cpus, ok := report.claimCPUs(claimUID)
			g.Expect(ok).To(gomega.BeTrue(), "claim %s is not reported: %+v", claimUID, report.Claims)
			claimCPUs = cpus
		}, pollTimeoutRule, pollIntervalRule).Should(gomega.Succeed())
		gomega.Expect(claimCPUs.Intersection(staticPool).IsEmpty()).To(gomega.BeTrue(),
			"the claim %s was placed on the pool %s", claimCPUs.String(), staticPool.String())

		localPool := numaLocalPool(nodeCPUInfo, staticPool, claimCPUs)
		gomega.Expect(localPool.IsEmpty()).To(gomega.BeFalse(),
			"the pool %s has no CPUs on the claim's NUMA nodes (claim %s)", staticPool.String(), claimCPUs.String())

		ginkgo.By("verifying the container holds claim plus local pool, and the kernel agrees")
		want := claimCPUs.Union(localPool)
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		gomega.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(want))
		gomega.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(want))

		ginkgo.By("verifying DRA_SHARED_CPUS names exactly the local pool")
		gomega.Expect(alloc.SharedEnvCPUs).ToNot(gomega.BeEmpty(), "the container did not receive DRA_SHARED_CPUS")
		envShared, err := cpuset.Parse(alloc.SharedEnvCPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse DRA_SHARED_CPUS %q", alloc.SharedEnvCPUs)
		gomega.Expect(envShared).To(cpusetmatchers.Equal(localPool))
	})

	ginkgo.It("should report the pool and keep it away from claims", func(ctx context.Context) {
		report, err := getPlacements(ctx, rootFxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		reportPool, err := cpuset.Parse(report.SharedPoolCPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse reported sharedPoolCPUs %q", report.SharedPoolCPUs)
		gomega.Expect(reportPool).To(cpusetmatchers.Equal(staticPool))

		shared, err := cpuset.Parse(report.SharedCPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(shared).To(cpusetmatchers.Equal(staticPool),
			"with a static pool the shared mask must be the pool itself")

		reserved, err := cpuset.Parse(report.ReservedCPUs)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reserved).To(cpusetmatchers.HaveNoOverlapWith(staticPool),
			"the pool is not reserved: reserved CPUs host nothing, the pool hosts shared containers")

		for _, node := range report.NUMANodes {
			free, err := cpuset.Parse(node.FreeCPUs)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(free).To(cpusetmatchers.HaveNoOverlapWith(staticPool),
				"NUMA node %d counts pool CPUs as free for claims", node.NUMANodeID)
		}
	})
})
