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

package pod_test

import (
	"testing"

	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateSyncWithClaimTemplate(t *testing.T) {
	const podUID = types.UID("pod-uid")
	claimOf := func(name string, uid, ownerUID types.UID) *resourcev1.ResourceClaim {
		return &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      name,
			UID:       uid,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "owner", UID: ownerUID, Controller: new(true)},
			},
		}}
	}

	for _, tc := range []struct {
		name    string
		claims  []runtime.Object
		wantUID types.UID
		wantErr string
	}{
		{
			name:    "the claim the pod controls",
			claims:  []runtime.Object{claimOf("other", "other-claim-uid", "other-pod-uid"), claimOf("tester-claim", "claim-uid", podUID)},
			wantUID: "claim-uid",
		},
		{
			name:    "no claim the pod controls",
			claims:  []runtime.Object{claimOf("other", "other-claim-uid", "other-pod-uid")},
			wantErr: "no resource claim is controlled by pod test/tester@node-a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := fake.NewClientset(tc.claims...)
			template := &resourcev1.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{Name: "template"}}
			// The fake runs no kubelet, so the pod is created running.
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "tester", UID: podUID},
				Spec:       v1.PodSpec{NodeName: "node-a"},
				Status:     v1.PodStatus{Phase: v1.PodRunning},
			}

			got, uid, err := e2epod.CreateSyncWithClaimTemplate(t.Context(), cs, template, pod)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantUID, uid)
			require.Equal(t, "tester", got.Name)
			_, err = cs.ResourceV1().ResourceClaimTemplates("test").Get(t.Context(), "template", metav1.GetOptions{})
			require.NoError(t, err, "the template is created in the pod's namespace")
		})
	}
}
