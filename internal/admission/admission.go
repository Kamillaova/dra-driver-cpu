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

// Package admission judges a ResourceClaim's spec by the rules of the
// driver's opaque configuration.
package admission

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	allocationapi "github.com/kubernetes-sigs/dra-driver-cpu/api/allocation"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
)

// Kind is what a request's devices give it: a share of CPUs, CPUs of its own,
// or both where its results land on devices of either.
type Kind struct {
	Shared bool
	Owned  bool
	// Exclusive says, in a refusal, what makes an owned request exclusive.
	Exclusive string
}

// ClassKinds returns the kind of every DeviceClass the chart renders for cfg,
// named as the chart names them, from the partitions of every profile: a
// class's kind is the same on every node of a supported cluster.
func ClassKinds(driverName string, cfg driverconfig.Config) map[string]Kind {
	kinds := map[string]Kind{}
	if cfg.PartitionsUsed() || len(cfg.Profiles) > 0 {
		kinds[driverName] = Kind{Shared: true}
	} else {
		kinds[driverName] = Kind{Owned: true, Exclusive: fmt.Sprintf("exclusive request (class %s, no cpuPartitions)", driverName)}
	}
	lists := [][]driverconfig.CPUPartition{cfg.CPUPartitions}
	for _, profile := range cfg.Profiles {
		lists = append(lists, profile.CPUPartitions)
	}
	for _, list := range lists {
		for _, partition := range list {
			class := driverName + "." + partition.Name
			switch {
			case partition.Name == device.DefaultPartitionName:
			case partition.Role == device.PARTITION_ROLE_DEFAULT:
				kinds[class] = Kind{Shared: true}
			case partition.Role == device.PARTITION_ROLE_EXCLUSIVE:
				kinds[class] = Kind{Owned: true, Exclusive: fmt.Sprintf("exclusive request (class %s)", class)}
			}
		}
	}
	return kinds
}

// ParseLimit reads a written limit: LimitMax, or a quantity above zero. Any
// other value is no limit, and ok is false.
func ParseLimit(limit v1alpha1.Limit) (quantity resource.Quantity, isMax, ok bool) {
	if limit == v1alpha1.LimitMax {
		return resource.Quantity{}, true, true
	}
	quantity, err := resource.ParseQuantity(string(limit))
	if err != nil || quantity.Sign() <= 0 {
		return resource.Quantity{}, false, false
	}
	return quantity, false, true
}

// judged is a request, or an alternative of a firstAvailable request, whose
// kind the caller knows.
type judged struct {
	ref       string
	base      string
	kind      Kind
	amount    resource.Quantity
	hasAmount bool
	spread    string
	limit     *v1alpha1.Limit
}

// noun names the request, or the alternative the scheduler may pick.
func (j judged) noun() string {
	if j.ref != j.base {
		return fmt.Sprintf("subrequest %q", j.ref)
	}
	return fmt.Sprintf("request %q", j.ref)
}

type entry struct {
	index    int
	requests []string
	config   allocationapi.Config
}

// reaches applies resourceclaim.ConfigForResult's matching to a claim's own
// configuration: an entry naming no request reaches every one, and one naming
// a request reaches its alternatives.
func (e entry) reaches(ref string) bool {
	return len(e.requests) == 0 || slices.Contains(e.requests, ref) || slices.Contains(e.requests, resourceclaim.BaseRequestRef(ref))
}

// CheckClaim judges the requests and alternatives of spec whose kind kindOf
// knows, by the class each names, and the driver's configuration entries that
// reach them. Each alternative of a firstAvailable request is judged as a
// request, so a claim is refused whichever alternative the scheduler picks.
func CheckClaim(spec *resourceapi.ResourceClaimSpec, driverName string, kindOf func(ref, class string) (Kind, bool)) error {
	requests := judgedRequests(spec, driverName, kindOf)
	if len(requests) == 0 {
		return nil
	}

	var entries []entry
	for i, config := range spec.Devices.Config {
		if config.Opaque == nil || config.Opaque.Driver != driverName {
			continue
		}
		e := entry{index: i, requests: config.Requests}
		if !slices.ContainsFunc(requests, func(j judged) bool { return e.reaches(j.ref) }) {
			continue
		}
		parsed, err := allocationapi.ParseOpaqueConfig(config.Opaque.Parameters.Raw)
		if err != nil {
			return fmt.Errorf("config entry #%d: %w", i, err)
		}
		e.config = parsed
		entries = append(entries, e)
	}

	for _, e := range entries {
		if e.config.Limit == nil {
			continue
		}
		if _, _, ok := ParseLimit(*e.config.Limit); ok {
			continue
		}
		written := string(*e.config.Limit)
		if written == "" {
			written = `""`
		}
		return fmt.Errorf("%s: invalid limit %s", e.target(requests), written)
	}

	for i := range requests {
		var limited []entry
		for _, e := range entries {
			if e.config.Limit != nil && e.reaches(requests[i].ref) {
				limited = append(limited, e)
			}
		}
		if len(limited) > 1 {
			return fmt.Errorf("%s: cpuConfig.limit set by two config entries (#%d and #%d); set it in one entry", requests[i].noun(), limited[0].index, limited[1].index)
		}
		if len(limited) == 1 {
			requests[i].limit = limited[0].config.Limit
		}
	}

	for _, j := range requests {
		if err := checkLimit(j); err != nil {
			return err
		}
	}
	for _, j := range requests {
		if err := checkCapped(j, requests); err != nil {
			return err
		}
	}
	return nil
}

// target names, in a refusal of the entry's limit, the first judged request it
// reaches, or the alternative where the entry names that alternative.
func (e entry) target(requests []judged) string {
	for _, j := range requests {
		if !e.reaches(j.ref) {
			continue
		}
		if j.ref != j.base && slices.Contains(e.requests, j.ref) {
			return j.noun()
		}
		return fmt.Sprintf("request %q", j.base)
	}
	return ""
}

func judgedRequests(spec *resourceapi.ResourceClaimSpec, driverName string, kindOf func(ref, class string) (Kind, bool)) []judged {
	var requests []judged
	add := func(ref, base, class string, capacity *resourceapi.CapacityRequirements, mode resourceapi.DeviceAllocationMode, count int64) {
		kind, ok := kindOf(ref, class)
		if !ok {
			return
		}
		j := judged{ref: ref, base: base, kind: kind, spread: spreadOf(mode, count)}
		if capacity != nil {
			j.amount, j.hasAmount = device.LookupConsumedCapacity(capacity.Requests, driverName)
		}
		requests = append(requests, j)
	}
	for _, request := range spec.Devices.Requests {
		if request.Exactly != nil {
			add(request.Name, request.Name, request.Exactly.DeviceClassName, request.Exactly.Capacity, request.Exactly.AllocationMode, request.Exactly.Count)
		}
		for _, sub := range request.FirstAvailable {
			add(resourceclaim.CreateSubRequestRef(request.Name, sub.Name), request.Name, sub.DeviceClassName, sub.Capacity, sub.AllocationMode, sub.Count)
		}
	}
	return requests
}

func spreadOf(mode resourceapi.DeviceAllocationMode, count int64) string {
	switch {
	case mode == resourceapi.DeviceAllocationModeAll:
		return "`allocationMode: All`, which may take several devices"
	case count > 1:
		return fmt.Sprintf("`count: %d`", count)
	}
	return ""
}

// checkLimit refuses a numeric limit on owned CPUs, which it would throttle,
// and on a share a limit with no amount or below its amount, as Kubernetes
// refuses a container limit below its request.
func checkLimit(j judged) error {
	if j.limit == nil {
		return nil
	}
	quantity, isMax, _ := ParseLimit(*j.limit)
	if j.kind.Owned && !isMax {
		return fmt.Errorf("request %q: limit %s on %s; only `limit: max` is allowed", j.ref, *j.limit, j.kind.Exclusive)
	}
	if !j.kind.Shared {
		return nil
	}
	if !j.hasAmount {
		return fmt.Errorf("%s: limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero", j.noun())
	}
	if !isMax && quantity.Cmp(j.amount) < 0 {
		return fmt.Errorf("%s: limit %s is below the amount %s", j.noun(), *j.limit, j.amount.String())
	}
	return nil
}

// capOf returns the cap a share asks for, as its refusals name it: its
// numeric limit as written, or else its amount above zero.
func capOf(j judged) (string, bool) {
	if !j.kind.Shared {
		return "", false
	}
	if j.limit != nil {
		return string(*j.limit), *j.limit != v1alpha1.LimitMax
	}
	return j.amount.String(), j.hasAmount && j.amount.Sign() > 0
}

// checkCapped refuses a capped share that may take more than one device, since
// a cgroup has one cpu.max, or that shares the claim with another request of
// this driver, all of which land in the one cgroup of the container using the
// claim. Two alternatives of one request are never beside each other.
func checkCapped(j judged, requests []judged) error {
	limit, capped := capOf(j)
	if !capped {
		return nil
	}
	if j.spread != "" {
		return fmt.Errorf("%s has limit of %s with %s; a limit can be applied to only one device: ask for one device, or remove the limit", j.noun(), limit, j.spread)
	}
	var others []string
	for _, other := range requests {
		if other.base != j.base && !slices.Contains(others, other.base) {
			others = append(others, other.base)
		}
	}
	if len(others) == 1 {
		return fmt.Errorf("%s has limit of %s, so it cannot be used together with request %q: remove the limit", j.noun(), limit, others[0])
	}
	if len(others) > 1 {
		quoted := make([]string, 0, len(others))
		for _, request := range others {
			quoted = append(quoted, strconv.Quote(request))
		}
		return fmt.Errorf("%s has limit of %s, so it cannot be used together with requests %s: remove the limit", j.noun(), limit, strings.Join(quoted, ", "))
	}
	return nil
}
