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
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/cpuset"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
)

type claimReader interface {
	AllocatedClaims() ([]*resourceapi.ResourceClaim, error)
	IsProjectedDeallocated(claimUID types.UID) bool
	GetProjectedClaims() (*v1alpha1.ProjectedClaims, error)
}

type claimConfigMapReader struct {
	lister     corelisters.ConfigMapNamespaceLister
	nodeName   string
	driverName string

	mu          sync.Mutex
	lastSeenGen int64
}

func (r *claimConfigMapReader) getProjected() (*v1alpha1.ProjectedClaims, error) {
	cmName := v1alpha1.ProjectedClaimsConfigMapName(r.nodeName)
	cm, err := r.lister.Get(cmName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	data, ok := cm.Data[v1alpha1.ProjectedClaimsKey]
	if !ok || data == "" {
		return nil, nil
	}
	var projected v1alpha1.ProjectedClaims
	if err := json.Unmarshal([]byte(data), &projected); err != nil {
		return nil, nil
	}
	if projected.APIVersion != "" && projected.APIVersion != v1alpha1.APIVersion {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if projected.Generation < r.lastSeenGen {
		return nil, nil
	}
	r.lastSeenGen = projected.Generation
	return &projected, nil
}

func (r *claimConfigMapReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	projected, err := r.getProjected()
	if err != nil || projected == nil {
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
	projected, err := r.getProjected()
	if err != nil || projected == nil {
		return false
	}
	for _, pc := range projected.Claims {
		if types.UID(pc.UID) == claimUID {
			return pc.State == v1alpha1.ClaimStateDeallocated
		}
	}
	return false
}

func (r *claimConfigMapReader) GetProjectedClaims() (*v1alpha1.ProjectedClaims, error) {
	return r.getProjected()
}

func watchAllocatedClaims(ctx context.Context, cp *CPUDriver) (claimReader, error) {
	if cp.kubeClient == nil {
		return nil, nil
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

	cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
			cp.reconcileMakeRoomTargets(context.Background())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
			cp.reconcileMakeRoomTargets(context.Background())
		},
		DeleteFunc: func(obj interface{}) {
			cp.republishStaleSlicesLocking(context.Background())
			cp.reconcileMakeRoomTargets(context.Background())
		},
	})

	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for typ, synced := range factory.WaitForCacheSync(syncCtx.Done()) {
		if !synced {
			return nil, fmt.Errorf("failed to sync configmap informer cache for %v", typ)
		}
	}
	return reader, nil
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
