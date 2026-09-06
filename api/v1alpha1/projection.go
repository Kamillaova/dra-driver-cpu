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

package v1alpha1

// ProjectedClaims is what one node's claims look like to a writer that can list
// them cluster-wide, projected into a ConfigMap the driver on that node reads.
//
// The driver does not watch ResourceClaims itself, so this is how it learns of a
// claim that is allocated but not yet prepared. A missing or stale projection
// means no such knowledge, which is what a driver running without a projector
// has.
type ProjectedClaims struct {
	// APIVersion specifies the schema version. Should be "v1alpha1".
	APIVersion string `json:"apiVersion"`
	// Generation rises with every write, so a reader can tell a projection it
	// has already seen from one it has not, and an older one from a newer.
	Generation int64 `json:"generation"`
	// Claims holds every claim allocated to this node that names the driver.
	Claims []ProjectedClaim `json:"claims"`
}

// ProjectedClaim is one claim as the projection carries it.
type ProjectedClaim struct {
	// UID identifies the claim, and is what the driver keys its own records by.
	UID string `json:"uid"`
	// Namespace and Name locate the claim for an operator reading the
	// projection by hand.
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// Devices are the results the allocator recorded for this driver, which
	// never change once written.
	Devices []ProjectedDevice `json:"devices"`
	// CPUConfig is the claim's opaque configuration, as the driver would have
	// parsed it at Prepare.
	CPUConfig CPUConfig `json:"cpuConfig"`
	// State says whether the claim still holds its allocation.
	State ClaimState `json:"state"`
}

// ProjectedDevice is one device one request of a claim was given.
type ProjectedDevice struct {
	Request string `json:"request"`
	Pool    string `json:"pool"`
	Device  string `json:"device"`
}

// ClaimState is how far through its life a projected claim is.
type ClaimState string

const (
	// ClaimStateAllocated means the claim holds an allocation, whether or not
	// any node has prepared it.
	ClaimStateAllocated ClaimState = "Allocated"
	// ClaimStateDeallocated means the allocation was cleared or the claim
	// deleted, which is when the allocator stops charging its devices.
	ClaimStateDeallocated ClaimState = "Deallocated"
)

// ProjectedClaimsKey is the ConfigMap data key the projection lives under.
const ProjectedClaimsKey = "claims.json"

// projectedClaimsNamePrefix keeps a projection from colliding with an unrelated
// ConfigMap that happens to be named after a node.
const projectedClaimsNamePrefix = "dra-cpu-claims-"

// ProjectedClaimsConfigMapName is the ConfigMap holding one node's projection.
// Node names are DNS subdomains, so the result is one too, and a fleet whose
// node names approach the 253-character limit needs a shorter prefix rather
// than a longer name.
func ProjectedClaimsConfigMapName(nodeName string) string {
	return projectedClaimsNamePrefix + nodeName
}
