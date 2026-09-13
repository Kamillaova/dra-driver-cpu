/*
Copyright 2026 The Kubernetes Authors.

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

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	resourceapi "k8s.io/api/resource/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/cpuset"
)

type claimReader interface {
	AllocatedClaims() ([]*resourceapi.ResourceClaim, error)
	IsProjectedDeallocated(claimUID types.UID) bool
}

type claimConfigMapReader struct {
	lister     corelisters.ConfigMapNamespaceLister
	nodeName   string
	driverName string
}

// getProjected is what the scheduler has projected for this node, and whether
// there is anything to read at all. A ConfigMap that has not been written yet,
// carries no projection, or is written to an API version this driver does not
// know are the same answer -- nothing -- and none of them is a failure. One that
// will not parse is: it was written by something, and silently reading a node as
// empty would hand out CPUs a claim already holds.
func (r *claimConfigMapReader) getProjected() (v1alpha1.ProjectedClaims, bool, error) {
	var none v1alpha1.ProjectedClaims
	cmName := v1alpha1.ProjectedClaimsConfigMapName(r.nodeName)
	cm, err := r.lister.Get(cmName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return none, false, nil
		}
		return none, false, err
	}
	data, ok := cm.Data[v1alpha1.ProjectedClaimsKey]
	if !ok || data == "" {
		return none, false, nil
	}
	var projected v1alpha1.ProjectedClaims
	if err := json.Unmarshal([]byte(data), &projected); err != nil {
		return none, false, fmt.Errorf("the projected claims of node %q do not parse: %w", r.nodeName, err)
	}
	if projected.APIVersion != "" && projected.APIVersion != v1alpha1.APIVersion {
		return none, false, nil
	}
	// Whatever the ConfigMap says now, including a generation lower than one
	// already seen. An informer's cache never moves backwards, so a lower
	// generation is not a stale read being replayed -- it is a projector that
	// restarted and began counting again. Holding a floor against it would
	// leave this driver on a projection nothing can ever supersede, silently,
	// for as long as the node lives.
	return projected, true, nil
}

func (r *claimConfigMapReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	projected, ok, err := r.getProjected()
	if err != nil || !ok {
		return nil, err
	}
	var claims []*resourceapi.ResourceClaim
	for _, pc := range projected.Claims {
		if pc.State != v1alpha1.ClaimStateAllocated {
			continue
		}
		var results []resourceapi.DeviceRequestAllocationResult
		for _, dev := range pc.Devices {
			results = append(results, resourceapi.DeviceRequestAllocationResult{
				Driver:  r.driverName,
				Request: dev.Request,
				Pool:    dev.Pool,
				Device:  dev.Device,
			})
		}
		claim := &resourceapi.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				UID:       types.UID(pc.UID),
				Namespace: pc.Namespace,
				Name:      pc.Name,
			},
			Status: resourceapi.ResourceClaimStatus{
				Allocation: &resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: results,
					},
				},
			},
		}
		claims = append(claims, claim)
	}
	return claims, nil
}

func (r *claimConfigMapReader) IsProjectedDeallocated(claimUID types.UID) bool {
	projected, ok, err := r.getProjected()
	if err != nil || !ok {
		return false
	}
	for _, pc := range projected.Claims {
		if types.UID(pc.UID) == claimUID {
			return pc.State == v1alpha1.ClaimStateDeallocated
		}
	}
	return false
}

// watchAllocatedClaims gives the driver its reader and keeps it fed from the
// ConfigMap the scheduler projects for this node.
//
// The reader is installed before the informer's handlers are, and under the lock
// that guards it, because those handlers republish -- and a republish that runs
// while the field is still nil reports a node holding no claims at all.
func watchAllocatedClaims(ctx context.Context, cp *CPUDriver) error {
	if cp.kubeClient == nil {
		return nil
	}
	ns := cp.namespace
	if ns == "" {
		ns = metav1.NamespaceDefault
	}
	cmName := v1alpha1.ProjectedClaimsConfigMapName(cp.nodeName)
	factory := informers.NewSharedInformerFactoryWithOptions(cp.kubeClient, 0,
		informers.WithNamespace(ns),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", cmName).String()
		}),
	)
	cmInformer := factory.Core().V1().ConfigMaps()
	reader := &claimConfigMapReader{
		lister:     cmInformer.Lister().ConfigMaps(ns),
		nodeName:   cp.nodeName,
		driverName: cp.driverName,
	}

	cp.applyMu.Lock()
	cp.claimReader = reader
	cp.applyMu.Unlock()

	if _, err := cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
		},
		DeleteFunc: func(obj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
		},
	}); err != nil {
		return fmt.Errorf("cannot watch the projected claim ConfigMap of node %q: %w", cp.nodeName, err)
	}

	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for typ, synced := range factory.WaitForCacheSync(syncCtx.Done()) {
		if !synced {
			return fmt.Errorf("failed to sync configmap informer cache for %v", typ)
		}
	}
	return nil
}

func (cp *CPUDriver) allocatedUnpreparedCPUs(numaNodeID int) cpuset.CPUSet {
	if cp.claimReader == nil {
		return cpuset.New()
	}
	claims, err := cp.claimReader.AllocatedClaims()
	if err != nil {
		return cpuset.New()
	}
	unprepared := cpuset.New()
	for _, claim := range claims {
		if claim.Status.Allocation == nil {
			continue
		}
		if _, ok := cp.cpuAllocationStore.GetClaimRecord(claim.UID); ok {
			continue
		}
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver != cp.driverName {
				continue
			}
			var devCPUs cpuset.CPUSet
			if cpus, ok := cp.topology.deviceNameToCPUs[result.Device]; ok {
				devCPUs = cpus
			} else if cpuID, ok := cp.topology.deviceNameToCPUID[result.Device]; ok {
				devCPUs = cpuset.New(cpuID)
			} else {
				continue
			}
			unprepared = unprepared.Union(devCPUs)
		}
	}
	if numaNodeID >= 0 && cp.topology.cpuTopology != nil {
		nodeCPUs := cp.topology.cpuTopology.CPUDetails.CPUsInNUMANodes(numaNodeID)
		return unprepared.Intersection(nodeCPUs)
	}
	return unprepared
}
