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
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/cpuset"
	"sigs.k8s.io/yaml"
)

// upstreamChart is the helm.sh/chart label of upstream's last release, which
// the upgrade lane installs.
const upstreamChart = "dra-driver-cpu-0.3.1"

var _ = ginkgo.Describe("Upgrade from upstream's last release", ginkgo.Label("upgrade"), ginkgo.Serial, ginkgo.Ordered, func() {
	const claimCPUs = 2

	var (
		rootFxt           *fixture.Fixture
		fxt               *fixture.Fixture
		dracpuTesterImage string
		driverImage       string
		release           string
		targetNode        *v1.Node
		cfg               driverConfigValues
		configBefore      map[string]any
		capacitiesBefore  map[string]string
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		daemonSet, err := rootFxt.K8SClientset.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get dracpu daemonset")
		if chart := daemonSet.Labels["helm.sh/chart"]; chart != upstreamChart {
			ginkgo.Skip(fmt.Sprintf("the driver runs chart %q, not upstream's last release %q; create the cluster with DRACPU_E2E_LANE=upgrade", chart, upstreamChart))
		}
		driverImage = os.Getenv("DRACPU_E2E_DRIVER_IMAGE")
		gomega.Expect(driverImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_DRIVER_IMAGE")
		release = daemonSet.Labels["app.kubernetes.io/instance"]
		gomega.Expect(release).ToNot(gomega.BeEmpty(), "daemonset dracpu names no Helm release")

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		cfg = getDriverConfig(ctx, rootFxt.K8SClientset)
		configBefore = driverConfigFile(ctx, rootFxt.K8SClientset)
		capacitiesBefore = deviceCapacities(nodeDevices(ctx, gomega.Default, rootFxt.K8SClientset, targetNode.Name))
		gomega.Expect(capacitiesBefore).ToNot(gomega.BeEmpty(), "upstream's driver publishes no device on node %s", targetNode.Name)
		rootFxt.Log.Info("devices upstream's driver publishes", "node", targetNode.Name, "capacities", capacitiesBefore)

		fxt = rootFxt.WithPrefix("upgrade")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)
		ginkgo.DeferCleanup(setUnschedulable, rootFxt.K8SClientset, targetNode.Name, false)
	})

	ginkgo.It("should prepare an exclusive claim under upstream's last release", func(ctx context.Context) {
		fixture.By("running a pod with a claim of %d exclusive CPUs on node %s", claimCPUs, targetNode.Name)
		pod, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfg, claimCPUs, "upgrade-before")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		gomega.Expect(alloc.CPUAssigned).To(cpusetmatchers.HaveSize(claimCPUs), "pod %s", e2epod.Identify(pod))
		gomega.Expect(alloc.CPUReported).To(cpusetmatchers.Equal(alloc.CPUAssigned), "pod %s does not run on the CPUs upstream's driver gave its claim", e2epod.Identify(pod))
	})

	ginkgo.It("should upgrade the driver of the drained node and keep upstream's configuration and devices", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		fixture.By("draining node %s", targetNode.Name)
		drainNode(ctx, cs, targetNode.Name)
		gomega.Eventually(func(g gomega.Gomega) {
			claims, err := cs.ResourceV1().ResourceClaims(fxt.Namespace.Name).List(ctx, metav1.ListOptions{})
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(claims.Items).To(gomega.BeEmpty(), "the claims of the drained pods are not deleted")
		}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())

		fixture.By("upgrading release %s to this tree's chart with the same values", release)
		colon := strings.LastIndex(driverImage, ":")
		gomega.Expect(colon).To(gomega.BeNumerically(">", strings.LastIndex(driverImage, "/")), "image %q has no tag", driverImage)
		helm := os.Getenv("DRACPU_E2E_HELM")
		if helm == "" {
			helm = "helm"
		}
		out, err := exec.CommandContext(ctx, helm, "upgrade", release, filepath.Join("..", "..", "deployment", "helm", "dra-driver-cpu"),
			"--namespace", daemonSetNamespace, "--reset-then-reuse-values",
			"--set", "image.repository="+driverImage[:colon], "--set", "image.tag="+driverImage[colon+1:],
			"--wait", "--timeout", "5m").CombinedOutput()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "helm upgrade: %s", out)
		expectDriverRolledOut(ctx, cs, targetNode.Name, driverImage)
		setUnschedulable(ctx, cs, targetNode.Name, false)

		fixture.By("checking the driver keeps upstream's configuration")
		gomega.Expect(driverConfigFile(ctx, cs)).To(gomega.Equal(configBefore), "the upgrade changed the driver's configuration")
		upgraded := getDriverConfig(ctx, cs)
		gomega.Expect(upgraded.CPUPartitions).To(gomega.BeEmpty())
		gomega.Expect(upgraded.Profiles).To(gomega.BeEmpty())

		fixture.By("checking node %s publishes the devices upstream's driver published", targetNode.Name)
		gomega.Eventually(func(g gomega.Gomega) {
			devices := nodeDevices(ctx, g, cs, targetNode.Name)
			for _, dev := range devices {
				g.Expect(dev.Attributes[device.AttributePartition].StringValue).To(gomega.HaveValue(gomega.Equal(device.DefaultPartitionName)),
					"device %s is not yet published by the upgraded driver, which names its partition", dev.Name)
			}
			g.Expect(deviceCapacities(devices)).To(gomega.Equal(capacitiesBefore))
		}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should prepare a new claim on the drained node with the upgraded driver", func(ctx context.Context) {
		cs := fxt.K8SClientset
		shared := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)

		fixture.By("running a pod with a claim of %d exclusive CPUs on node %s", claimCPUs, targetNode.Name)
		pod, claimUID := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfg, claimCPUs, "upgrade-after")
		exclusive := getTesterPodCPUAllocation(cs, ctx, pod).CPUAssigned
		gomega.Expect(exclusive).To(cpusetmatchers.HaveSize(claimCPUs), "pod %s", e2epod.Identify(pod))
		gomega.Eventually(observeAssignedCPUs(ctx, fxt, shared)).WithTimeout(time.Minute).WithPolling(5*time.Second).Should(cpusetmatchers.HaveNoOverlapWith(exclusive),
			"the best-effort pod %s runs on the claim's CPUs", e2epod.Identify(shared))

		fixture.By("checking the claim's status names the CPUs the upgraded driver gave it")
		claim := claimByUID(ctx, cs, fxt.Namespace.Name, claimUID)
		gomega.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(), "claim %s is not allocated", claim.Name)
		var results int
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver == driverName {
				results++
			}
		}
		placed := cpuset.New()
		var entries int
		for _, entry := range claim.Status.Devices {
			if entry.Driver != driverName {
				continue
			}
			entries++
			gomega.Expect(entry.Data).ToNot(gomega.BeNil(), "the entry of device %s carries no data", entry.Device)
			var data v1alpha1.ClaimPlacementStatus
			gomega.Expect(json.Unmarshal(entry.Data.Raw, &data)).To(gomega.Succeed(), "the entry of device %s is not a ClaimPlacementStatus", entry.Device)
			gomega.Expect(data.Partition).To(gomega.Equal(device.DefaultPartitionName), "the entry of device %s", entry.Device)
			cpus, err := cpuset.Parse(data.CPUSet)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "the entry of device %s", entry.Device)
			placed = placed.Union(cpus)
		}
		gomega.Expect(entries).To(gomega.Equal(results), "claim %s's status does not carry one entry of %s per result", claim.Name, driverName)
		gomega.Expect(placed).To(cpusetmatchers.Equal(exclusive), "claim %s's status does not name the CPUs pod %s runs on", claim.Name, e2epod.Identify(pod))
	})
})

func setUnschedulable(ctx context.Context, cs kubernetes.Interface, nodeName string, unschedulable bool) {
	ginkgo.GinkgoHelper()
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, unschedulable)
	_, err := cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot set node %s unschedulable to %t", nodeName, unschedulable)
}

// drainNode cordons the node and evicts every pod on it but the DaemonSets'
// and the static ones, as kubectl drain does, and waits for them to go.
func drainNode(ctx context.Context, cs kubernetes.Interface, nodeName string) {
	ginkgo.GinkgoHelper()
	setUnschedulable(ctx, cs, nodeName, true)
	pods, err := cs.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list the pods of node %s", nodeName)
	var evicted []v1.Pod
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if (owner != nil && owner.Kind == "DaemonSet") || pod.Annotations[v1.MirrorPodAnnotationKey] != "" {
			continue
		}
		err := cs.CoreV1().Pods(pod.Namespace).EvictV1(ctx, &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}})
		if apierrors.IsNotFound(err) {
			continue
		}
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot evict pod %s", e2epod.Identify(&pod))
		evicted = append(evicted, pod)
	}
	for _, pod := range evicted {
		gomega.Expect(e2epod.WaitToBeDeleted(ctx, cs, pod.Namespace, pod.Name)).To(gomega.Succeed(), "evicted pod %s is not deleted", e2epod.Identify(&pod))
	}
}

// driverConfigFile returns the driver's configuration file as the chart wrote
// it into the dracpu-config ConfigMap.
func driverConfigFile(ctx context.Context, cs kubernetes.Interface) map[string]any {
	ginkgo.GinkgoHelper()
	cm, err := cs.CoreV1().ConfigMaps(daemonSetNamespace).Get(ctx, "dracpu-config", metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get the driver's ConfigMap")
	var config map[string]any
	gomega.Expect(yaml.Unmarshal([]byte(cm.Data["config.yaml"]), &config)).To(gomega.Succeed(), "cannot parse the driver's configuration")
	return config
}

// deviceCapacities maps each device's name to its capacities, request
// policies included, as JSON.
func deviceCapacities(devices []resourcev1.Device) map[string]string {
	ginkgo.GinkgoHelper()
	capacities := make(map[string]string, len(devices))
	for _, dev := range devices {
		data, err := json.Marshal(dev.Capacity)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot encode the capacities of device %s", dev.Name)
		capacities[dev.Name] = string(data)
	}
	return capacities
}

// expectDriverRolledOut waits for the dracpu DaemonSet to finish its rollout,
// and for its pod on the node to run image.
func expectDriverRolledOut(ctx context.Context, cs kubernetes.Interface, nodeName, image string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		daemonSet, err := cs.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(daemonSet.Status.ObservedGeneration).To(gomega.BeNumerically(">=", daemonSet.Generation))
		g.Expect(daemonSet.Status).To(gomega.And(
			gomega.HaveField("UpdatedNumberScheduled", daemonSet.Status.DesiredNumberScheduled),
			gomega.HaveField("NumberAvailable", daemonSet.Status.DesiredNumberScheduled),
		), "daemonset dracpu has not rolled out")
		pod, err := e2epod.GetDRACPUPod(ctx, cs, nodeName)
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "the driver on node %s", nodeName)
		g.Expect(pod.Spec.Containers).To(gomega.ContainElement(gomega.HaveField("Image", image)), "the driver on node %s does not run %s", nodeName, image)
	}).WithTimeout(5 * time.Minute).WithPolling(driverPodPollInterval).Should(gomega.Succeed())
}

func claimByUID(ctx context.Context, cs kubernetes.Interface, namespace string, uid types.UID) *resourcev1.ResourceClaim {
	ginkgo.GinkgoHelper()
	claims, err := cs.ResourceV1().ResourceClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list the claims of namespace %s", namespace)
	for i := range claims.Items {
		if claims.Items[i].UID == uid {
			return &claims.Items[i]
		}
	}
	ginkgo.Fail(fmt.Sprintf("no claim of UID %s in namespace %s", uid, namespace))
	return nil
}
