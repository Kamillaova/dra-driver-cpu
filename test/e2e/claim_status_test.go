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

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	e2eclient "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/client"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
)

// The API server re-encodes a claim's opaque parameters when it applies or
// patches the claim's status, then refuses the write as a change of the
// allocation where they were sent in another form
// (kubernetes/kubernetes#143901). Each claim here is created over protobuf,
// which keeps the parameters' bytes, in a form Go's JSON encoder does not
// give them; the driver must still prepare it and write its status.
var _ = ginkgo.Describe("Claim status", ginkgo.Label("claim-status"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		protobufClient    kubernetes.Interface
		dracpuTesterImage string
		cfg               driverConfigValues
		targetNode        *v1.Node
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		config, err := e2eclient.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create Kubernetes config")
		config.ContentType = runtime.ContentTypeProtobuf
		config.AcceptContentTypes = runtime.ContentTypeProtobuf
		clientset, err := kubernetes.NewForConfig(config)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create a protobuf client")
		protobufClient, err = e2eclient.NewK8SClientset(e2eclient.WithClientset(clientset))
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create a labeling protobuf client")

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		cfg = getDriverConfig(ctx, rootFxt.K8SClientset)
	})

	for _, tc := range []struct {
		name   string
		labels ginkgo.Labels
		shares bool
		params []resourcev1.OpaqueDeviceConfiguration
	}{{
		name:   "should prepare a claim whose parameters list their keys out of order, and keep them as sent",
		params: []resourcev1.OpaqueDeviceConfiguration{dracpuParams(`{"cpuConfig":{"limit":"max"},"apiVersion":"v1alpha1"}`)},
	}, {
		name:   "should prepare a claim whose parameters hold whitespace, and keep them as sent",
		params: []resourcev1.OpaqueDeviceConfiguration{dracpuParams("{ \"apiVersion\": \"v1alpha1\",\n  \"cpuConfig\": { \"limit\": \"max\" } }")},
	}, {
		name:   "should prepare a claim whose parameters hold escaped characters, and keep them as sent",
		params: []resourcev1.OpaqueDeviceConfiguration{dracpuParams(`{"apiVersion":"v1` + jsonEscape('a') + `lpha1","cpuConfig":{"limit":"m` + jsonEscape('a') + `x"}}`)},
	}, {
		name:   "should prepare a claim whose parameters write a limit as 1.0, and keep them as sent",
		labels: ginkgo.Label("partitions"),
		shares: true,
		params: []resourcev1.OpaqueDeviceConfiguration{dracpuParams(`{"apiVersion":"v1alpha1","cpuConfig":{"limit":1.0}}`)},
	}, {
		name: "should prepare a claim beside another driver's parameters holding < and an escaped character, and keep them as sent",
		params: []resourcev1.OpaqueDeviceConfiguration{
			dracpuParams(`{"apiVersion":"v1alpha1","cpuConfig":{"limit":"max"}}`),
			{Driver: "other.example.com", Parameters: runtime.RawExtension{Raw: []byte(`{"note": "<` + jsonEscape('é') + `>"}`)}},
		},
	}} {
		ginkgo.It(tc.name, tc.labels, func(ctx context.Context) {
			if tc.shares && !cfg.effectiveFor(targetNode).partitionsUsed() {
				ginkgo.Skip("node " + targetNode.Name + " runs no partitions, so its requests are exclusive and take no limit but max; create the cluster with DRACPU_E2E_LANE=partitions")
			}
			fxt := rootFxt.WithPrefix("claim-status")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			spec := makeResourceClaimSpec(1, cfg.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED)
			for _, params := range tc.params {
				entry := resourcev1.DeviceClaimConfiguration{DeviceConfiguration: resourcev1.DeviceConfiguration{Opaque: params.DeepCopy()}}
				if params.Driver == driverName {
					entry.Requests = []string{spec.Devices.Requests[0].Name}
				}
				spec.Devices.Config = append(spec.Devices.Config, entry)
			}
			claim, err := protobufClient.ResourceV1().ResourceClaims(fxt.Namespace.Name).Create(ctx, &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "claim-status"},
				Spec:       spec,
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create the claim")

			fixture.By("running a pod on claim %s on node %s", claim.Name, targetNode.Name)
			pod := makeTesterPodWithNamedClaim(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, cfg.PublishNodeAllocatableResourceMapping, claim)
			pod, err = e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "the pod on claim %s does not run", claim.Name)

			fixture.By("reading claim %s back over protobuf", claim.Name)
			claim, err = protobufClient.ResourceV1().ResourceClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(), "claim %s of running pod %s is not allocated", claim.Name, e2epod.Identify(pod))
			for _, params := range tc.params {
				gomega.Expect(claim.Status.Allocation.Devices.Config).To(gomega.ContainElement(gomega.HaveField("Opaque", gomega.And(
					gomega.HaveField("Driver", params.Driver),
					gomega.HaveField("Parameters.Raw", gomega.WithTransform(func(raw []byte) string { return string(raw) }, gomega.Equal(string(params.Parameters.Raw)))),
				))), "the allocation of claim %s does not keep %s's parameters as sent", claim.Name, params.Driver)
			}

			var results []resourcev1.DeviceRequestAllocationResult
			for _, result := range claim.Status.Allocation.Devices.Results {
				if result.Driver == driverName {
					results = append(results, result)
				}
			}
			var entries []resourcev1.AllocatedDeviceStatus
			for _, entry := range claim.Status.Devices {
				if entry.Driver == driverName {
					entries = append(entries, entry)
				}
			}
			gomega.Expect(results).ToNot(gomega.BeEmpty(), "claim %s has no result on %s", claim.Name, driverName)
			gomega.Expect(entries).To(gomega.HaveLen(len(results)), "claim %s's status does not carry one entry of %s per result", claim.Name, driverName)
			for _, result := range results {
				gomega.Expect(entries).To(gomega.ContainElement(gomega.And(
					gomega.HaveField("Pool", result.Pool),
					gomega.HaveField("Device", result.Device),
					gomega.HaveField("Data", gomega.Not(gomega.BeNil())),
				)), "claim %s's status has no entry for device %s", claim.Name, result.Device)
			}
			for _, entry := range entries {
				var data v1alpha1.ClaimPlacementStatus
				gomega.Expect(json.Unmarshal(entry.Data.Raw, &data)).To(gomega.Succeed(), "the entry of device %s is not a ClaimPlacementStatus", entry.Device)
				gomega.Expect(data.Kind).To(gomega.Equal(v1alpha1.ClaimPlacementStatusKind))
				gomega.Expect(data.Partition).ToNot(gomega.BeEmpty(), "the entry of device %s names no partition", entry.Device)
			}

			gomega.Expect(claim.ManagedFields).To(gomega.ContainElement(gomega.And(
				gomega.HaveField("Manager", driverName),
				gomega.HaveField("Operation", metav1.ManagedFieldsOperationUpdate),
				gomega.HaveField("Subresource", "status"),
			)), "claim %s's managedFields show no update of its status by %s", claim.Name, driverName)
			gomega.Expect(claim.ManagedFields).ToNot(gomega.ContainElement(gomega.And(
				gomega.HaveField("Manager", driverName),
				gomega.HaveField("Operation", metav1.ManagedFieldsOperationApply),
			)), "%s applied to claim %s", driverName, claim.Name)
		})
	}
})

// dracpuParams returns raw as the driver's opaque parameters, its bytes as
// written.
func dracpuParams(raw string) resourcev1.OpaqueDeviceConfiguration {
	return resourcev1.OpaqueDeviceConfiguration{Driver: driverName, Parameters: runtime.RawExtension{Raw: []byte(raw)}}
}

// jsonEscape writes r as a JSON \u escape, which Go's encoder writes as the
// character itself.
func jsonEscape(r rune) string {
	return fmt.Sprintf(`\u%04x`, r)
}
