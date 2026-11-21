/*
Copyright 2025 The Kubernetes Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
	cdiSpec "tags.cncf.io/container-device-interface/specs-go"
)

const (
	cdiSpecVersion  = "0.8.0"
	cdiVendor       = "dra.k8s.io"
	cdiClass        = "cpu"
	cdiEnvVarPrefix = "DRA_CPUSET"
	cdiSpecDir      = "/var/run/cdi"

	// cdiEnvDynamicValue is the value of every per-request variable, which
	// only names its claim and request: the CPUs are in the driver's record.
	cdiEnvDynamicValue = "dynamic"

	// A claim's record lives in an annotation of its spec. Spec annotations
	// "are CDI-specific and do not affect container metadata" (CDI specs-go).
	cdiRecordAnnotation = "dra.cpu/record"
)

// cdiClaimRecord is what the spec of a claim records of its preparation.
type cdiClaimRecord struct {
	Namespace string
	Name      string
	Requests  []cdiRequestRecord
}

type cdiRequestRecord struct {
	Request string
	CPUs    cpuset.CPUSet
	Role    store.Role
	Results []cdiResultRecord
	// Cap is the request's cap in millicores, 0 for none.
	Cap int64
}

type cdiResultRecord struct {
	Device   string             `json:"device"`
	Consumed *resource.Quantity `json:"consumed,omitempty"`
}

// cdiRecord is a claim's record as dra.cpu/record holds it.
type cdiRecord struct {
	Namespace  string         `json:"namespace"`
	Name       string         `json:"name"`
	Placements []cdiPlacement `json:"placements"`
}

// cdiPlacement is one request of dra.cpu/record as the spec holds it.
type cdiPlacement struct {
	Request string             `json:"request"`
	CPUs    string             `json:"cpus"`
	Role    store.Role         `json:"role,omitempty"`
	Results []cdiResultRecord  `json:"results,omitempty"`
	Cap     *resource.Quantity `json:"cap,omitempty"`
}

func (r cdiClaimRecord) storeRecord() store.ClaimRecord {
	record := store.ClaimRecord{Namespace: r.Namespace, Name: r.Name}
	for _, request := range r.Requests {
		allocation := store.RequestAllocation{Request: request.Request, CPUs: request.CPUs, Role: request.Role, Cap: request.Cap}
		if request.Role == store.RoleShared {
			for _, result := range request.Results {
				allocation.Devices = append(allocation.Devices, result.Device)
			}
		}
		record.Requests = append(record.Requests, allocation)
	}
	return record
}

func encodeClaimRecord(record cdiClaimRecord) (string, error) {
	encoded := cdiRecord{Namespace: record.Namespace, Name: record.Name, Placements: make([]cdiPlacement, 0, len(record.Requests))}
	for _, request := range record.Requests {
		placement := cdiPlacement{Request: request.Request, CPUs: request.CPUs.String(), Role: request.Role, Results: request.Results}
		if request.Cap > 0 {
			placement.Cap = resource.NewMilliQuantity(request.Cap, resource.DecimalSI)
		}
		encoded.Placements = append(encoded.Placements, placement)
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// decodeClaimRecord reads the record dra.cpu/record holds. It refuses a
// request recorded with no CPU: NRI sends an empty cpuset as no change to the
// container's CPUs.
func decodeClaimRecord(value string) (cdiClaimRecord, error) {
	var decoded cdiRecord
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return cdiClaimRecord{}, fmt.Errorf("failed to parse %s: %w", cdiRecordAnnotation, err)
	}
	if decoded.Namespace == "" {
		return cdiClaimRecord{}, fmt.Errorf("%s records no namespace of the claim", cdiRecordAnnotation)
	}
	if decoded.Name == "" {
		return cdiClaimRecord{}, fmt.Errorf("%s records no name of the claim", cdiRecordAnnotation)
	}
	if len(decoded.Placements) == 0 {
		return cdiClaimRecord{}, fmt.Errorf("%s records no request", cdiRecordAnnotation)
	}
	record := cdiClaimRecord{Namespace: decoded.Namespace, Name: decoded.Name}
	for _, placement := range decoded.Placements {
		if placement.Request == "" || slices.ContainsFunc(record.Requests, func(r cdiRequestRecord) bool {
			return r.Request == placement.Request && r.Role == placement.Role
		}) {
			return cdiClaimRecord{}, fmt.Errorf("%s records request %q twice in one role or without a name", cdiRecordAnnotation, placement.Request)
		}
		cpus, err := cpuset.Parse(placement.CPUs)
		if err != nil {
			return cdiClaimRecord{}, fmt.Errorf("failed to parse the CPUs of request %q: %w", placement.Request, err)
		}
		if cpus.IsEmpty() {
			return cdiClaimRecord{}, fmt.Errorf("%s records no CPU for request %q", cdiRecordAnnotation, placement.Request)
		}
		if placement.Role != store.RoleExclusive && placement.Role != store.RoleShared {
			return cdiClaimRecord{}, fmt.Errorf("%s records request %q with role %q, which this driver does not know", cdiRecordAnnotation, placement.Request, placement.Role)
		}
		if placement.Role == store.RoleShared && len(placement.Results) == 0 {
			return cdiClaimRecord{}, fmt.Errorf("%s records shared request %q with no device", cdiRecordAnnotation, placement.Request)
		}
		var capMilli int64
		if placement.Cap != nil {
			if placement.Role != store.RoleShared || placement.Cap.Sign() <= 0 {
				return cdiClaimRecord{}, fmt.Errorf("%s records cap %s on request %q, which only a shared request above 0 has", cdiRecordAnnotation, placement.Cap.String(), placement.Request)
			}
			capMilli = placement.Cap.MilliValue()
		}
		record.Requests = append(record.Requests, cdiRequestRecord{Request: placement.Request, CPUs: cpus, Role: placement.Role, Results: placement.Results, Cap: capMilli})
	}
	return record, nil
}

// CdiManager handles the lifecycle of CDI allocations for the driver.
type CdiManager struct {
	cache      *cdiapi.Cache
	cdiKind    string
	driverName string
}

// NewCdiManager creates a manager for the driver's CDI allocations.
func NewCdiManager(logger logr.Logger, driverName string, cdiDir string) (*CdiManager, error) {
	cache, err := cdiapi.NewCache(
		cdiapi.WithSpecDirs(cdiDir),
		// Disabled because we manage state entirely via the filesystem
		// and write individual stateless files per claim.
		cdiapi.WithAutoRefresh(false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CDI cache: %w", err)
	}

	c := &CdiManager{
		cache:      cache,
		cdiKind:    fmt.Sprintf("%s/%s", cdiVendor, cdiClass),
		driverName: driverName,
	}

	logger.Info("Initialized CDI manager", "driverName", driverName, "cdiDir", cdiDir)
	return c, nil
}

// getSpecName generates a unique, sanitized filename for a specific device allocation.
func (c *CdiManager) getSpecName(deviceName string) string {
	return cdiapi.GenerateTransientSpecName(cdiVendor, cdiClass, deviceName) + ".json"
}

// AddDevice writes a dedicated CDI spec file for a single device allocation.
func (c *CdiManager) AddDevice(logger logr.Logger, deviceName string, envVar string) error {
	specName := c.getSpecName(deviceName)

	spec := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    c.cdiKind,
		Devices: []cdiSpec.Device{
			{
				Name: deviceName,
				ContainerEdits: cdiSpec.ContainerEdits{
					Env: []string{envVar},
				},
			},
		},
	}

	if err := c.cache.WriteSpec(spec, specName); err != nil {
		return fmt.Errorf("failed to write CDI spec %q: %w", specName, err)
	}

	logger.V(4).Info("Added CDI device", "deviceName", deviceName, "specName", specName, "env", envVar)
	return nil
}

// AddClaimDevices writes the spec of a claim: a device for each entry of
// envByDevice, injecting that variable, and the claim's record. One spec file
// holds them, and the CDI cache writes it through a temporary file and a
// rename, so a claim's devices and its record land together or not at all.
func (c *CdiManager) AddClaimDevices(logger logr.Logger, claimUID types.UID, envByDevice map[string]string, record cdiClaimRecord) error {
	specName := c.getSpecName(getCDIDeviceName(claimUID))
	encoded, err := encodeClaimRecord(record)
	if err != nil {
		return fmt.Errorf("failed to record the requests of claim %q: %w", claimUID, err)
	}
	spec := &cdiSpec.Spec{
		Version:     cdiSpecVersion,
		Kind:        c.cdiKind,
		Annotations: map[string]string{cdiRecordAnnotation: encoded},
	}
	for _, deviceName := range slices.Sorted(maps.Keys(envByDevice)) {
		spec.Devices = append(spec.Devices, cdiSpec.Device{
			Name: deviceName,
			ContainerEdits: cdiSpec.ContainerEdits{
				Env: []string{envByDevice[deviceName]},
			},
		})
	}

	if err := c.cache.WriteSpec(spec, specName); err != nil {
		return fmt.Errorf("failed to write CDI spec %q: %w", specName, err)
	}

	logger.V(4).Info("Added CDI devices of claim", "claimUID", claimUID, "specName", specName, "numDevices", len(spec.Devices), "record", encoded)
	return nil
}

// Refresh reloads the CDI specs managed by the cache.
func (c *CdiManager) Refresh() error {
	if err := c.cache.Refresh(); err != nil {
		return fmt.Errorf("failed to refresh CDI cache: %w", err)
	}
	return nil
}

// GetDeviceEnv returns the environment edits for a specific CDI device allocation
// from the current cache. Call Refresh before lookup to load the latest on-disk specs.
func (c *CdiManager) GetDeviceEnv(deviceName string) ([]string, error) {
	device := c.cache.GetDevice(cdiparser.QualifiedName(cdiVendor, cdiClass, deviceName))
	if device == nil {
		return nil, fmt.Errorf("failed to find CDI device %q", deviceName)
	}
	return append([]string{}, device.ContainerEdits.Env...), nil
}

// GetDeviceAllocations returns what the spec of a claim records. A spec that
// upstream's driver wrote records only the claim's cpuset, in the variable of
// its one device, and reads as one request without a name. Call Refresh before
// lookup to load the latest on-disk specs.
func (c *CdiManager) GetDeviceAllocations(claimUID types.UID) (cdiClaimRecord, error) {
	specName := c.getSpecName(getCDIDeviceName(claimUID))
	for _, spec := range c.cache.GetVendorSpecs(cdiVendor) {
		if filepath.Base(spec.GetPath()) != specName {
			continue
		}
		if value, ok := spec.Annotations[cdiRecordAnnotation]; ok {
			record, err := decodeClaimRecord(value)
			if err != nil {
				return cdiClaimRecord{}, fmt.Errorf("CDI spec %q: %w", specName, err)
			}
			return record, nil
		}
		device := spec.GetDevice(getCDIDeviceName(claimUID))
		if device == nil {
			return cdiClaimRecord{}, fmt.Errorf("CDI spec %q records no CPUs of claim %q", specName, claimUID)
		}
		cpus, err := cdiSpecCPUs(logr.Discard(), claimUID, device.ContainerEdits.Env)
		if err != nil {
			return cdiClaimRecord{}, err
		}
		return cdiClaimRecord{Requests: []cdiRequestRecord{{CPUs: cpus}}}, nil
	}
	return cdiClaimRecord{}, fmt.Errorf("%w: %q of claim %q", errNoClaimSpec, specName, claimUID)
}

var errNoClaimSpec = errors.New("failed to find CDI spec")

// ListClaims returns, in order, the UIDs of the claims whose spec is on disk,
// in this driver's form or in upstream's. Call Refresh first to load the
// latest on-disk specs.
func (c *CdiManager) ListClaims() []types.UID {
	prefix := strings.TrimSuffix(c.getSpecName(getCDIDeviceName("")), ".json")
	var claims []types.UID
	for _, spec := range c.cache.GetVendorSpecs(cdiVendor) {
		name := filepath.Base(spec.GetPath())
		uid := types.UID(strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".json"))
		if uid != "" && name == c.getSpecName(getCDIDeviceName(uid)) {
			claims = append(claims, uid)
		}
	}
	slices.Sort(claims)
	return claims
}

// RemoveClaimDevices deletes the spec of a claim, which holds every device of
// the claim in this driver's form and in upstream's.
func (c *CdiManager) RemoveClaimDevices(logger logr.Logger, claimUID types.UID) error {
	specName := c.getSpecName(getCDIDeviceName(claimUID))

	if err := c.cache.RemoveSpec(specName); err != nil {
		return fmt.Errorf("failed to remove CDI spec %q: %w", specName, err)
	}

	logger.V(4).Info("Removed CDI devices of claim", "claimUID", claimUID, "specName", specName)
	return nil
}
