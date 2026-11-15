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
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/client"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	resourcev1apply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	nodeNameExtra             = "authentication.kubernetes.io/node-name"
	mappingDroppedCondition   = "CPUAllocatableMappingDropped"
	policyCheckReason         = "E2EAdmissionPolicyCheck"
	policyCheckMarker         = "e2e.dra.cpu/admission-policy"
	otherClaimDriver          = "dra.memory"
	policyCheckSliceDriver    = "admission-policy.e2e.dra.cpu"
	tokenNamesNoNodeRefusal   = "the driver's service account token names no node: run the driver in a pod, with the token the kubelet mounts into it"
	policyCheckFieldManager   = "e2e-admission-policy"
	claimBindingAuthorization = `requires resource="resourceclaims/binding"`
	claimDriverAuthorization  = `requires resource="resourceclaims/driver"`
)

// The writes of this spec are made with tokens of the driver's service
// account, not through the fixture's client, so it removes what it creates.
var _ = ginkgo.Describe("Admission policies on the driver's writes", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, ginkgo.Label("admission-policies"), func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		otherNode         string
		policyPrefix      string
		asDriver          kubernetes.Interface
		withoutNode       kubernetes.Interface
		dracpuTesterImage string
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create root fixture: %v", err)
		cs := rootFxt.K8SClientset

		daemonSet, err := cs.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get dracpu daemonset")
		policyPrefix = daemonSet.Name
		_, err = cs.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, policyPrefix+"-node-status", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			ginkgo.Skip("the chart was installed with rbac.create false, which renders no admission policy")
		}
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		targetNode, err = e2enode.PickWorker(ctx, cs, 5*time.Second, time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		for _, node := range nodes.Items {
			if node.Name != targetNode.Name {
				otherNode = node.Name
				break
			}
		}
		gomega.Expect(otherNode).ToNot(gomega.BeEmpty(), "the spec needs a second node")
		rootFxt.Log.Info("nodes", "target", targetNode.Name, "other", otherNode)

		driverPod, err := e2epod.GetDRACPUPod(ctx, cs, targetNode.Name)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		config, err := client.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		account := "system:serviceaccount:" + driverPod.Namespace + ":" + driverPod.Spec.ServiceAccountName

		ginkgo.By("taking a token of the driver's service account bound to its pod on " + targetNode.Name)
		asDriver = serviceAccountClient(ctx, cs, config, driverPod, true)
		expectIdentity(ctx, asDriver, account, targetNode.Name)
		ginkgo.By("taking a token of the driver's service account bound to no pod")
		withoutNode = serviceAccountClient(ctx, cs, config, driverPod, false)
		expectIdentity(ctx, withoutNode, account, "")
	})

	ginkgo.It("should admit only the driver's own condition on its own Node's status", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		policy := policyPrefix + "-node-status"
		node := targetNode.Name

		before, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		saved := findNodeCondition(before, mappingDroppedCondition)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			restore := []byte(`{"status":{"conditions":[{"type":"` + mappingDroppedCondition + `","$patch":"delete"}]}}`)
			if saved != nil {
				restore = nodeConditionPatch(*saved)
			}
			_, err := cs.CoreV1().Nodes().PatchStatus(ctx, node, restore)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			after, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			if saved == nil {
				gomega.Expect(findNodeCondition(after, mappingDroppedCondition)).To(gomega.BeNil())
			} else {
				gomega.Expect(findNodeCondition(after, mappingDroppedCondition).Reason).To(gomega.Equal(saved.Reason))
			}
		})

		now := metav1.NewTime(time.Now().Truncate(time.Second))
		written := v1.NodeCondition{Type: mappingDroppedCondition, Status: v1.ConditionFalse, Reason: policyCheckReason,
			Message: "written by the admission policy e2e", LastHeartbeatTime: now, LastTransitionTime: now}

		ginkgo.By("patching the driver's condition on its own Node")
		_, err = asDriver.CoreV1().Nodes().PatchStatus(ctx, node, nodeConditionPatch(written))
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		stored, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(findNodeCondition(stored, mappingDroppedCondition)).To(gomega.HaveField("Reason", policyCheckReason))

		otherCondition := written
		otherCondition.Type = policyCheckReason
		changedMessage := written
		changedMessage.Message = "written without the node's name"
		refusals := []struct {
			name    string
			client  kubernetes.Interface
			node    string
			patch   []byte
			message string
			kept    func(g gomega.Gomega, n *v1.Node)
		}{
			{
				name:    "the driver's condition on another Node",
				client:  asDriver,
				node:    otherNode,
				patch:   nodeConditionPatch(written),
				message: fmt.Sprintf("the driver on node '%s' may write only its own node's status, not node '%s'", node, otherNode),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(findNodeCondition(n, mappingDroppedCondition)).To(gomega.Or(gomega.BeNil(), gomega.Not(gomega.HaveField("Reason", policyCheckReason))))
				},
			},
			{
				name:    "a token that names no node",
				client:  withoutNode,
				node:    node,
				patch:   nodeConditionPatch(changedMessage),
				message: tokenNamesNoNodeRefusal,
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(findNodeCondition(n, mappingDroppedCondition)).To(gomega.HaveField("Message", written.Message))
				},
			},
			{
				name:    "another condition type",
				client:  asDriver,
				node:    node,
				patch:   nodeConditionPatch(otherCondition),
				message: fmt.Sprintf("the driver on node '%s' may change only the node conditions %s", node, mappingDroppedCondition),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(findNodeCondition(n, policyCheckReason)).To(gomega.BeNil())
				},
			},
			{
				name:    "another status field",
				client:  asDriver,
				node:    node,
				patch:   []byte(`{"status":{"nodeInfo":{"kernelVersion":"` + policyCheckMarker + `"}}}`),
				message: fmt.Sprintf("the driver on node '%s' may change only its own conditions in its node's status", node),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(n.Status.NodeInfo.KernelVersion).ToNot(gomega.Equal(policyCheckMarker))
				},
			},
			{
				name:    "a label",
				client:  asDriver,
				node:    node,
				patch:   []byte(`{"metadata":{"labels":{"` + policyCheckMarker + `":"refused"}}}`),
				message: fmt.Sprintf("the driver on node '%s' may not change its node's metadata", node),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(n.Labels).ToNot(gomega.HaveKey(policyCheckMarker))
				},
			},
			{
				name:    "an annotation",
				client:  asDriver,
				node:    node,
				patch:   []byte(`{"metadata":{"annotations":{"` + policyCheckMarker + `":"refused"}}}`),
				message: fmt.Sprintf("the driver on node '%s' may not change its node's metadata", node),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(n.Annotations).ToNot(gomega.HaveKey(policyCheckMarker))
				},
			},
			{
				name:    "a finalizer",
				client:  asDriver,
				node:    node,
				patch:   []byte(`{"metadata":{"finalizers":["` + policyCheckMarker + `"]}}`),
				message: fmt.Sprintf("the driver on node '%s' may not change its node's metadata", node),
				kept: func(g gomega.Gomega, n *v1.Node) {
					g.Expect(n.Finalizers).ToNot(gomega.ContainElement(policyCheckMarker))
				},
			},
		}
		for _, tc := range refusals {
			ginkgo.By("refusing " + tc.name)
			_, err := tc.client.CoreV1().Nodes().PatchStatus(ctx, tc.node, tc.patch)
			expectPolicyRefusal(err, policy, tc.message)
			n, err := cs.CoreV1().Nodes().Get(ctx, tc.node, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			tc.kept(gomega.Default, n)
		}
	})

	ginkgo.It("should admit ResourceSlices of the driver's own node only", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		policy := policyPrefix + "-resourceslices"

		ginkgo.By("finding the ResourceSlices the driver wrote for " + targetNode.Name + " with the token mounted into its pod")
		published, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{FieldSelector: "spec.driver=" + driverName})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(published.Items).To(gomega.ContainElement(gomega.HaveField("Spec.NodeName", gomega.HaveValue(gomega.Equal(targetNode.Name)))),
			"the policy admitted none of the driver's own ResourceSlices for node %s", targetNode.Name)

		ginkgo.By("creating and deleting a ResourceSlice of its own node")
		created, err := asDriver.ResourceV1().ResourceSlices().Create(ctx, policyCheckSlice(targetNode.Name), metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		ginkgo.DeferCleanup(deleteSliceIfPresent, cs, created.Name)
		gomega.Expect(asDriver.ResourceV1().ResourceSlices().Delete(ctx, created.Name, metav1.DeleteOptions{})).To(gomega.Succeed())
		_, err = cs.ResourceV1().ResourceSlices().Get(ctx, created.Name, metav1.GetOptions{})
		gomega.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue(), "the slice is gone: %v", err)

		ginkgo.By("refusing a ResourceSlice of another node")
		refused, err := asDriver.ResourceV1().ResourceSlices().Create(ctx, policyCheckSlice(otherNode), metav1.CreateOptions{})
		if err == nil {
			ginkgo.DeferCleanup(deleteSliceIfPresent, cs, refused.Name)
		}
		expectPolicyRefusal(err, policy, fmt.Sprintf("the driver on node '%s' may write only ResourceSlices of its own node, not of node '%s'", targetNode.Name, otherNode))
	})

	ginkgo.It("should admit only the driver's own entries in the status of a claim on its node", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		policy := policyPrefix + "-resourceclaim-status"
		driverConfig := getDriverConfig(ctx, cs)
		if driverConfig.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && driverConfig.GroupBy == device.GROUP_BY_MACHINE {
			ginkgo.Skip("skipping this test in machine grouping mode as we do not configure opaque config in claim")
		}
		fxt := rootFxt.WithPrefix("admission")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)
		ns := fxt.Namespace.Name

		ginkgo.By("running a pod with a dra.cpu claim on " + targetNode.Name)
		claim, err := cs.ResourceV1().ResourceClaims(ns).Create(ctx, &resourcev1.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "claim-cpu"},
			Spec:       makeResourceClaimSpec(1, driverConfig.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED),
		}, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		pod := e2epod.PinToNode(&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pod-with-cpu-claim"},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{{
					Name:      "tester",
					Image:     dracpuTesterImage,
					Command:   []string{"/dracputester"},
					Resources: v1.ResourceRequirements{Claims: []v1.ResourceClaim{{Name: "cpu"}}},
				}},
				ResourceClaims: []v1.PodResourceClaim{{Name: "cpu", ResourceClaimName: new(claim.Name)}},
			},
		}, targetNode.Name)
		pod, err = e2epod.CreateSync(ctx, cs, pod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		claim, err = cs.ResourceV1().ResourceClaims(ns).Get(ctx, claim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(claim.Status.Allocation).ToNot(gomega.BeNil())
		gomega.Expect(claim.Status.Allocation.Devices.Results).To(gomega.HaveLen(1))
		result := claim.Status.Allocation.Devices.Results[0]
		gomega.Expect(claim.Status.ReservedFor).To(gomega.ContainElement(gomega.HaveField("UID", pod.UID)))

		ginkgo.By("writing a dra.cpu entry with a merge patch")
		var shareID *string
		if result.ShareID != nil {
			shareID = new(string(*result.ShareID))
		}
		entry := claimStatusEntry(result.Driver, result.Pool, result.Device, shareID, "merge-patch")
		_, err = asDriver.ResourceV1().ResourceClaims(ns).Patch(ctx, claim.Name, types.MergePatchType,
			mustJSON(map[string]any{"status": map[string]any{"devices": []resourcev1.AllocatedDeviceStatus{entry}}}), metav1.PatchOptions{}, "status")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(storedClaimDevices(ctx, cs, ns, claim.Name)).To(gomega.ConsistOf(gomega.HaveField("Data.Raw", gomega.MatchJSON(entry.Data.Raw))))

		ginkgo.By("writing the dra.cpu entry with a server-side apply")
		applied := resourcev1apply.AllocatedDeviceStatus().WithDriver(result.Driver).WithPool(result.Pool).WithDevice(result.Device).
			WithData(runtime.RawExtension{Raw: []byte(`{"written":"server-side-apply"}`)})
		if shareID != nil {
			applied = applied.WithShareID(*shareID)
		}
		_, err = asDriver.ResourceV1().ResourceClaims(ns).ApplyStatus(ctx,
			resourcev1apply.ResourceClaim(claim.Name, ns).WithStatus(resourcev1apply.ResourceClaimStatus().WithDevices(applied)),
			metav1.ApplyOptions{FieldManager: policyCheckFieldManager, Force: true})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(storedClaimDevices(ctx, cs, ns, claim.Name)).To(gomega.ConsistOf(gomega.HaveField("Data.Raw", gomega.MatchJSON(`{"written":"server-side-apply"}`))))

		// Kubernetes 1.36 and later refuse a node's write to the claim's
		// reservation or to another driver's entries in their own
		// authorization, before any admission policy sees it.
		ginkgo.By("refusing to clear the claim's reservation for its pod")
		_, err = asDriver.ResourceV1().ResourceClaims(ns).Patch(ctx, claim.Name, types.MergePatchType,
			[]byte(`{"status":{"reservedFor":null}}`), metav1.PatchOptions{}, "status")
		expectClaimStatusRefusal(err, policy, fmt.Sprintf("the driver on node '%s' may change only the devices of a claim's status", targetNode.Name), claimBindingAuthorization)
		claim, err = cs.ResourceV1().ResourceClaims(ns).Get(ctx, claim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(claim.Status.ReservedFor).To(gomega.ContainElement(gomega.HaveField("UID", pod.UID)))

		ginkgo.By("allocating a claim on " + targetNode.Name + " to dra.cpu and " + otherClaimDriver + ", as an administrator")
		shared, err := cs.ResourceV1().ResourceClaims(ns).Create(ctx, &resourcev1.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "claim-two-drivers"},
			Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
				{Name: "cpu", Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: driverName, Count: 1}},
				{Name: "mem", Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: otherClaimDriver, Count: 1}},
			}}},
		}, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		otherEntry := claimStatusEntry(otherClaimDriver, targetNode.Name, "memory0", nil, "other-driver")
		shared.Status = resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
					{Request: "cpu", Driver: driverName, Pool: result.Pool, Device: result.Device},
					{Request: "mem", Driver: otherClaimDriver, Pool: targetNode.Name, Device: "memory0"},
				}},
				NodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchFields: []v1.NodeSelectorRequirement{
					{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{targetNode.Name}},
				}}}},
			},
			Devices: []resourcev1.AllocatedDeviceStatus{otherEntry},
		}
		_, err = cs.ResourceV1().ResourceClaims(ns).UpdateStatus(ctx, shared, metav1.UpdateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("refusing a change to the other driver's entry")
		changedOther := claimStatusEntry(otherClaimDriver, targetNode.Name, "memory0", nil, "changed-by-dra.cpu")
		_, err = asDriver.ResourceV1().ResourceClaims(ns).Patch(ctx, shared.Name, types.MergePatchType,
			mustJSON(map[string]any{"status": map[string]any{"devices": []resourcev1.AllocatedDeviceStatus{changedOther}}}), metav1.PatchOptions{}, "status")
		expectClaimStatusRefusal(err, policy, fmt.Sprintf("the driver on node '%s' may change only the dra.cpu entries of a claim's status.devices", targetNode.Name), claimDriverAuthorization)
		gomega.Expect(storedClaimDevices(ctx, cs, ns, shared.Name)).To(gomega.ConsistOf(gomega.HaveField("Data.Raw", gomega.MatchJSON(otherEntry.Data.Raw))))

		ginkgo.By("writing a dra.cpu entry beside the other driver's, the two in a changed order")
		ownEntry := claimStatusEntry(driverName, result.Pool, result.Device, nil, "beside-another-driver")
		_, err = asDriver.ResourceV1().ResourceClaims(ns).Patch(ctx, shared.Name, types.MergePatchType,
			mustJSON(map[string]any{"status": map[string]any{"devices": []resourcev1.AllocatedDeviceStatus{ownEntry, otherEntry}}}), metav1.PatchOptions{}, "status")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(storedClaimDevices(ctx, cs, ns, shared.Name)).To(gomega.ConsistOf(
			gomega.HaveField("Data.Raw", gomega.MatchJSON(ownEntry.Data.Raw)),
			gomega.HaveField("Data.Raw", gomega.MatchJSON(otherEntry.Data.Raw)),
		))
	})
})

// serviceAccountClient returns a client that authenticates with a token of
// pod's service account, bound to pod or to nothing.
func serviceAccountClient(ctx context.Context, cs kubernetes.Interface, base *rest.Config, pod *v1.Pod, bindToPod bool) kubernetes.Interface {
	ginkgo.GinkgoHelper()
	request := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: new(int64(600))}}
	if bindToPod {
		request.Spec.BoundObjectRef = &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: pod.UID}
	}
	token, err := cs.CoreV1().ServiceAccounts(pod.Namespace).CreateToken(ctx, pod.Spec.ServiceAccountName, request, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	config := rest.AnonymousClientConfig(base)
	config.BearerToken = token.Status.Token
	tokenClient, err := kubernetes.NewForConfig(config)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	return tokenClient
}

// expectIdentity checks the user a client authenticates as and the node name
// the API server reads from its token, which the policies key on.
func expectIdentity(ctx context.Context, cs kubernetes.Interface, username, nodeName string) {
	ginkgo.GinkgoHelper()
	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	gomega.Expect(review.Status.UserInfo.Username).To(gomega.Equal(username))
	if nodeName == "" {
		gomega.Expect(review.Status.UserInfo.Extra).ToNot(gomega.HaveKey(nodeNameExtra))
		return
	}
	gomega.Expect(review.Status.UserInfo.Extra).To(gomega.HaveKeyWithValue(nodeNameExtra, authenticationv1.ExtraValue{nodeName}))
}

func expectPolicyRefusal(err error, policy, message string) {
	ginkgo.GinkgoHelper()
	gomega.Expect(err).To(gomega.HaveOccurred())
	gomega.Expect(err.Error()).To(gomega.ContainSubstring(
		fmt.Sprintf("ValidatingAdmissionPolicy '%s' with binding '%s' denied request: %s", policy, policy, message)))
}

// expectClaimStatusRefusal accepts the policy's refusal, or the API server's
// own where its authorization refuses the write first.
func expectClaimStatusRefusal(err error, policy, message, authorization string) {
	ginkgo.GinkgoHelper()
	gomega.Expect(err).To(gomega.HaveOccurred())
	gomega.Expect(err.Error()).To(gomega.Or(
		gomega.ContainSubstring(fmt.Sprintf("ValidatingAdmissionPolicy '%s' with binding '%s' denied request: %s", policy, policy, message)),
		gomega.ContainSubstring(authorization),
	))
}

func findNodeCondition(node *v1.Node, conditionType v1.NodeConditionType) *v1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == conditionType {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

// nodeConditionPatch is the strategic merge patch the driver sends for one
// condition.
func nodeConditionPatch(condition v1.NodeCondition) []byte {
	ginkgo.GinkgoHelper()
	return mustJSON(map[string]any{"status": map[string]any{"conditions": []v1.NodeCondition{condition}}})
}

func policyCheckSlice(nodeName string) *resourcev1.ResourceSlice {
	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "admission-policy-e2e-"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   policyCheckSliceDriver,
			Pool:     resourcev1.ResourcePool{Name: nodeName, ResourceSliceCount: 1},
			NodeName: new(nodeName),
		},
	}
}

func deleteSliceIfPresent(ctx context.Context, cs kubernetes.Interface, name string) {
	err := cs.ResourceV1().ResourceSlices().Delete(ctx, name, metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}
}

func claimStatusEntry(driver, pool, deviceName string, shareID *string, written string) resourcev1.AllocatedDeviceStatus {
	return resourcev1.AllocatedDeviceStatus{
		Driver:  driver,
		Pool:    pool,
		Device:  deviceName,
		ShareID: shareID,
		Data:    &runtime.RawExtension{Raw: []byte(`{"written":"` + written + `"}`)},
	}
}

func storedClaimDevices(ctx context.Context, cs kubernetes.Interface, namespace, name string) []resourcev1.AllocatedDeviceStatus {
	ginkgo.GinkgoHelper()
	claim, err := cs.ResourceV1().ResourceClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	return claim.Status.Devices
}

func mustJSON(v any) []byte {
	ginkgo.GinkgoHelper()
	data, err := json.Marshal(v)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	return data
}
