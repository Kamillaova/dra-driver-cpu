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
	"fmt"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/utils/cpuset"
)

type claimReader interface {
	AllocatedClaims() ([]*resourceapi.ResourceClaim, error)
}

type claimInformerReader struct {
	lister resourcelisters.ResourceClaimLister
}

func (r claimInformerReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	return r.lister.List(labels.Everything())
}

func watchAllocatedClaims(ctx context.Context, cp *CPUDriver) (claimReader, error) {
	if cp.kubeClient == nil {
		return nil, nil
	}
	factory := informers.NewSharedInformerFactory(cp.kubeClient, 0)
	informer := factory.Resource().V1().ResourceClaims()
	reader := claimInformerReader{lister: informer.Lister()}

	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for typ, synced := range factory.WaitForCacheSync(syncCtx.Done()) {
		if !synced {
			return nil, fmt.Errorf("failed to sync claim informer cache for %v", typ)
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
