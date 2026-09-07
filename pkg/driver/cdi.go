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
	"fmt"

	"github.com/go-logr/logr"
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

	// cdiCPUSetAnnotation records the CPUs a claim is currently pinned to.
	//
	// It lives in the CDI spec's device annotations rather than in its container
	// edits because CDI annotations are, per the specification, "CDI-specific and
	// do not affect container metadata": nothing is injected into the container,
	// so the driver can rewrite them at will. The injected env var cannot serve
	// this purpose, since a running container's environment is fixed at creation.
	cdiCPUSetAnnotation = "dra.cpu/cpuset"

	// cdiEnvDynamicValue stands in for a cpuset in the injected variable when a
	// claim's placement may change while its container runs.
	//
	// The variable is fixed when the container is created and cannot be corrected
	// afterwards, so any cpuset it named would become a lie the first time the
	// claim moved. This says as much, and the claim UID in the variable's name --
	// the part the driver actually needs -- is unaffected.
	//
	// It is also the value an upstream driver does least harm with, should one
	// take over these specs: it cannot parse it, so it passes the container over
	// and leaves its cpuset alone. A stale-looking cpuset would instead have it
	// reject the claim and, finding the container holding none, pin a guaranteed
	// container to the shared pool.
	cdiEnvDynamicValue = "dynamic"
)

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
		// and write individual stateless files per device.
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

// AddDevice writes a dedicated CDI spec file for a single device allocation,
// recording cpus as the device's current placement and injecting envVar into the
// container.
//
// The spec is written atomically by the CDI cache, so a concurrent reader sees
// either the previous placement or this one, never a mixture.
//
// CCX-FORK: upstream takes no cpus argument and records no placement.
func (c *CdiManager) AddDevice(logger logr.Logger, deviceName string, envVar string, cpus cpuset.CPUSet) error {
	specName := c.getSpecName(deviceName)

	spec := &cdiSpec.Spec{
		Version: cdiSpecVersion,
		Kind:    c.cdiKind,
		Devices: []cdiSpec.Device{
			{
				Name: deviceName,
				Annotations: map[string]string{
					cdiCPUSetAnnotation: cpus.String(),
				},
				ContainerEdits: cdiSpec.ContainerEdits{
					Env: []string{envVar},
				},
			},
		},
	}

	if err := c.cache.WriteSpec(spec, specName); err != nil {
		return fmt.Errorf("failed to write CDI spec %q: %w", specName, err)
	}

	logger.V(4).Info("Added CDI device", "deviceName", deviceName, "specName", specName, "env", envVar, "cpus", cpus.String())
	return nil
}

// GetDeviceCPUSet returns the CPUs recorded for a device allocation. Call Refresh
// before lookup to load the latest on-disk specs.
//
// Specs written before the driver recorded placement in an annotation carry it
// only in the injected env var, so fall back to parsing that. The spec file is
// driver-owned, so unlike a container's environment its env value is current.
func (c *CdiManager) GetDeviceCPUSet(deviceName string) (cpuset.CPUSet, error) {
	device := c.cache.GetDevice(cdiparser.QualifiedName(cdiVendor, cdiClass, deviceName))
	if device == nil {
		return cpuset.CPUSet{}, fmt.Errorf("failed to find CDI device %q", deviceName)
	}

	if recorded, ok := device.Annotations[cdiCPUSetAnnotation]; ok {
		cpus, err := cpuset.Parse(recorded)
		if err != nil {
			return cpuset.CPUSet{}, fmt.Errorf("failed to parse %s annotation %q of CDI device %q: %w", cdiCPUSetAnnotation, recorded, deviceName, err)
		}
		return cpus, nil
	}

	allocations, err := parseDRAEnvToClaimAllocations(logr.Discard(), device.ContainerEdits.Env)
	if err != nil {
		return cpuset.CPUSet{}, fmt.Errorf("failed to parse CDI device %q: %w", deviceName, err)
	}
	for _, cpus := range allocations {
		return cpus, nil
	}
	return cpuset.CPUSet{}, fmt.Errorf("CDI device %q records no CPU placement", deviceName)
}

// PreparedClaimAllocations returns the placement recorded on disk for every
// claim this driver previously prepared, keyed by claim UID. Call Refresh
// first to load the latest specs. A device this driver would not itself have
// generated, or whose recorded placement fails to parse, is skipped and
// logged rather than aborting recovery of every other claim.
func (c *CdiManager) PreparedClaimAllocations(logger logr.Logger) map[types.UID]cpuset.CPUSet {
	allocations := make(map[types.UID]cpuset.CPUSet)
	for _, qualified := range c.cache.ListDevices() {
		vendor, class, name, err := cdiparser.ParseQualifiedName(qualified)
		if err != nil || vendor != cdiVendor || class != cdiClass {
			continue
		}
		claimUID, ok := claimUIDFromDeviceName(name)
		if !ok {
			logger.V(2).Info("ignoring CDI device this driver would not have generated", "device", qualified)
			continue
		}
		cpus, err := c.GetDeviceCPUSet(name)
		if err != nil {
			logger.Error(err, "ignoring CDI device with an unrecoverable placement", "device", qualified)
			continue
		}
		allocations[claimUID] = cpus
	}
	return allocations
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

// RemoveDevice deletes the dedicated CDI spec file for a single device allocation.
func (c *CdiManager) RemoveDevice(logger logr.Logger, deviceName string) error {
	specName := c.getSpecName(deviceName)

	if err := c.cache.RemoveSpec(specName); err != nil {
		return fmt.Errorf("failed to remove CDI spec %q: %w", specName, err)
	}

	logger.V(4).Info("Removed CDI device", "deviceName", deviceName, "specName", specName)
	return nil
}
