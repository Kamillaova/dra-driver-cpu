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

package admission

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

const testDriverName = "dra.cpu"

func TestClassKinds(t *testing.T) {
	owned := func(why string) Kind { return Kind{Owned: true, Exclusive: "exclusive request (" + why + ")"} }
	shared := Kind{Shared: true}
	for _, tc := range []struct {
		name string
		cfg  driverconfig.Config
		want map[string]Kind
	}{{
		name: "without partitions dra.cpu is owned",
		want: map[string]Kind{"dra.cpu": owned("class dra.cpu, no cpuPartitions")},
	}, {
		name: "a fleet-wide list: dra.cpu shared, a class per named partition but a reserved one",
		cfg: driverconfig.Config{CPUPartitions: []driverconfig.CPUPartition{
			{Name: "default"},
			{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE},
			{Name: "infra", Role: device.PARTITION_ROLE_DEFAULT},
			{Name: "system", Role: device.PARTITION_ROLE_RESERVED},
		}},
		want: map[string]Kind{"dra.cpu": shared, "dra.cpu.vms": owned("class dra.cpu.vms"), "dra.cpu.infra": shared},
	}, {
		name: "every profile's partitions",
		cfg: driverconfig.Config{Profiles: map[string]driverconfig.Profile{
			"r7625": {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE}}},
			"spdk":  {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}, {Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE}}},
			"plain": {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}}},
		}},
		want: map[string]Kind{"dra.cpu": shared, "dra.cpu.vms": owned("class dra.cpu.vms"), "dra.cpu.spdk": owned("class dra.cpu.spdk")},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ClassKinds(testDriverName, tc.cfg))
		})
	}
}

// fleetKinds are the kinds of a chart whose dra.cpu is shared, beside an
// exclusive partition vms.
var fleetKinds = ClassKinds(testDriverName, driverconfig.Config{CPUPartitions: []driverconfig.CPUPartition{
	{Name: "default"},
	{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE},
}})

var plainKinds = ClassKinds(testDriverName, driverconfig.Config{})

func byClass(kinds map[string]Kind) func(ref, class string) (Kind, bool) {
	return func(_, class string) (Kind, bool) {
		kind, ok := kinds[class]
		return kind, ok
	}
}

type testRequest struct {
	name         string
	class        string
	amount       string
	count        int64
	all          bool
	alternatives []testRequest
}

type testEntry struct {
	requests []string
	params   string
	driver   string
}

func testSpec(requests []testRequest, entries ...testEntry) *resourceapi.ResourceClaimSpec {
	capacity := func(amount string) *resourceapi.CapacityRequirements {
		if amount == "" {
			return nil
		}
		return &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse(amount)}}
	}
	mode := func(r testRequest) resourceapi.DeviceAllocationMode {
		if r.all {
			return resourceapi.DeviceAllocationModeAll
		}
		return resourceapi.DeviceAllocationModeExactCount
	}
	spec := &resourceapi.ResourceClaimSpec{}
	for _, r := range requests {
		request := resourceapi.DeviceRequest{Name: r.name}
		if len(r.alternatives) == 0 {
			request.Exactly = &resourceapi.ExactDeviceRequest{DeviceClassName: r.class, Capacity: capacity(r.amount), AllocationMode: mode(r), Count: r.count}
		}
		for _, sub := range r.alternatives {
			request.FirstAvailable = append(request.FirstAvailable, resourceapi.DeviceSubRequest{Name: sub.name, DeviceClassName: sub.class, Capacity: capacity(sub.amount), AllocationMode: mode(sub), Count: sub.count})
		}
		spec.Devices.Requests = append(spec.Devices.Requests, request)
	}
	for _, e := range entries {
		driver := e.driver
		if driver == "" {
			driver = testDriverName
		}
		spec.Devices.Config = append(spec.Devices.Config, resourceapi.DeviceClaimConfiguration{
			Requests: e.requests,
			DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
				Driver: driver, Parameters: runtime.RawExtension{Raw: []byte(e.params)},
			}},
		})
	}
	return spec
}

func limit(value string) string {
	return `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": ` + value + `}}`
}

func TestCheckClaim(t *testing.T) {
	cpus := func(amount string) []testRequest {
		return []testRequest{{name: "cpus", class: "dra.cpu", amount: amount}}
	}
	for _, tc := range []struct {
		name     string
		kinds    map[string]Kind
		requests []testRequest
		entries  []testEntry
		wantErr  string
	}{{
		name:     "WA28: a limit of 0, a number",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`0`)}},
		wantErr:  `request "cpus": invalid limit 0`,
	}, {
		name:     "WA28: a limit of 0m, as written",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"0m"`)}},
		wantErr:  `request "cpus": invalid limit 0m`,
	}, {
		name:     `WA28: a limit of "0"`,
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"0"`)}},
		wantErr:  `request "cpus": invalid limit 0`,
	}, {
		name:     "WA28: a limit of 0 without an amount, before the rule on amounts",
		requests: cpus(""),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`0`)}},
		wantErr:  `request "cpus": invalid limit 0`,
	}, {
		name:     "a negative limit",
		requests: cpus("250m"),
		entries:  []testEntry{{params: limit(`-1`)}},
		wantErr:  `request "cpus": invalid limit -1`,
	}, {
		name:     "WA34: an empty limit",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`""`)}},
		wantErr:  `request "cpus": invalid limit ""`,
	}, {
		name:     "a limit that is no quantity",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"two"`)}},
		wantErr:  `request "cpus": invalid limit two`,
	}, {
		name:     "an invalid limit on an alternative the entry names",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "one", class: "dra.cpu", amount: "1"}, {name: "two", class: "dra.cpu", amount: "2"}}}},
		entries:  []testEntry{{requests: []string{"cpus/two"}, params: limit(`0`)}},
		wantErr:  `subrequest "cpus/two": invalid limit 0`,
	}, {
		name:     "WA11: two entries giving a request different limits",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"500m"`)}, {requests: []string{"cpus"}, params: limit(`"1"`)}},
		wantErr:  `request "cpus": cpuConfig.limit set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "WA44: two entries giving a request the same limit",
		requests: []testRequest{{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}},
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: limit(`"500m"`)}, {params: limit(`"500m"`)}},
		wantErr:  `request "vmm-cpu": cpuConfig.limit set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "WA29: a parent entry and an alternative's entry both giving a limit",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "two", class: "dra.cpu", amount: "250m"}, {name: "one", class: "dra.cpu", amount: "500m"}}}},
		entries:  []testEntry{{requests: []string{"cpus/two"}, params: limit(`"500m"`)}, {requests: []string{"cpus"}, params: limit(`"1"`)}},
		wantErr:  `subrequest "cpus/two": cpuConfig.limit set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "one entry per alternative",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "two", class: "dra.cpu", amount: "250m"}, {name: "one", class: "dra.cpu", amount: "500m"}}}},
		entries:  []testEntry{{requests: []string{"cpus/two"}, params: limit(`"500m"`)}, {requests: []string{"cpus/one"}, params: limit(`"1"`)}},
	}, {
		name:     "WA12: a limit without an amount on a share",
		requests: cpus(""),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"500m"`)}},
		wantErr:  `request "cpus": limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero`,
	}, {
		name:     "WB2: limit max without an amount on a share",
		requests: cpus(""),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"max"`)}},
		wantErr:  `request "cpus": limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero`,
	}, {
		name:     "WB2: limit max without an amount on owned CPUs",
		kinds:    plainKinds,
		requests: cpus(""),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"max"`)}},
	}, {
		name:     "an amount of 0 with a limit",
		requests: cpus("0"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"500m"`)}},
	}, {
		name:     "WA13: a limit below the amount",
		requests: cpus("500m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"250m"`)}},
		wantErr:  `request "cpus": limit 250m is below the amount 500m`,
	}, {
		name:     "WA36: a limit below a millicore below the amount, compared as written",
		requests: cpus("1m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"500u"`)}},
		wantErr:  `request "cpus": limit 500u is below the amount 1m`,
	}, {
		name:     "a limit quoted as written",
		requests: cpus("1"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`0.5`)}},
		wantErr:  `request "cpus": limit 0.5 is below the amount 1`,
	}, {
		name:     "a limit equal to the amount",
		requests: cpus("1"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`1`)}},
	}, {
		name:     "WA30: a parent limit over an alternative without an amount",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "two", class: "dra.cpu", amount: "250m"}, {name: "one", class: "dra.cpu"}}}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"500m"`)}},
		wantErr:  `subrequest "cpus/one": limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero`,
	}, {
		name:     "a parent limit max over an owned alternative without an amount",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "two", class: "dra.cpu", amount: "250m"}, {name: "owned", class: "dra.cpu.vms"}}}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"max"`)}},
	}, {
		name:     "WA37: an alternative judged whatever its sibling names",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "cpu", class: "dra.cpu"}, {name: "other", class: "gpu.example.com"}}}},
		entries:  []testEntry{{requests: []string{"cpus/cpu"}, params: limit(`"500m"`)}},
		wantErr:  `subrequest "cpus/cpu": limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero`,
	}, {
		name:     "WB29: another driver's alternative is not counted",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "cpu", class: "dra.cpu", amount: "500m"}, {name: "other", class: "gpu.example.com"}}}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"1"`)}},
	}, {
		name:     "a numeric limit on a class owned on every node",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{requests: []string{"vcpu"}, params: limit(`1`)}},
		wantErr:  "request \"vcpu\": limit 1 on exclusive request (class dra.cpu.vms); only `limit: max` is allowed",
	}, {
		name:     "a numeric limit on dra.cpu without partitions",
		kinds:    plainKinds,
		requests: cpus("2"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: limit(`"2"`)}},
		wantErr:  "request \"cpus\": limit 2 on exclusive request (class dra.cpu, no cpuPartitions); only `limit: max` is allowed",
	}, {
		name:     "an unscoped numeric limit while the claim holds an owned request",
		requests: []testRequest{{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{params: limit(`"500m"`)}},
		wantErr:  "request \"vcpu\": limit 500m on exclusive request (class dra.cpu.vms); only `limit: max` is allowed",
	}, {
		name:     "limit max on owned CPUs",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{params: limit(`"max"`)}},
	}, {
		name:     "WB1: an amount alone with count 2 is a cap on several devices",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "1", count: 2}},
		wantErr:  "request \"cpus\" has limit of 1 with `count: 2`; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name:     "a written limit with allocationMode: All",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "0", all: true}},
		entries:  []testEntry{{params: limit(`"500m"`)}},
		wantErr:  "request \"cpus\" has limit of 500m with `allocationMode: All`, which may take several devices; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name:     "limit max spanning devices",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "1", count: 2}},
		entries:  []testEntry{{params: limit(`"max"`)}},
	}, {
		name:     "a limit replacing the amount as the cap, beside another request",
		requests: []testRequest{{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: limit(`1`)}},
		wantErr:  `request "vmm-cpu" has limit of 1, so it cannot be used together with request "vcpu": remove the limit`,
	}, {
		name:     "an amount alone beside several requests",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "helper", class: "dra.cpu"}, {name: "io", class: "dra.cpu"}},
		wantErr:  `request "vmm-cpu" has limit of 250m, so it cannot be used together with requests "vcpu", "helper", "io": remove the limit`,
	}, {
		name:     "limit max beside another request",
		requests: []testRequest{{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: limit(`"max"`)}},
	}, {
		name: "WA41: an alternative capped on its pick beside another request",
		requests: []testRequest{
			{name: "vcpu", class: "dra.cpu.vms", amount: "2"},
			{name: "helper", alternatives: []testRequest{{name: "shared", class: "dra.cpu", amount: "500m"}, {name: "owned", class: "dra.cpu.vms", amount: "1"}}},
		},
		wantErr: `subrequest "helper/shared" has limit of 500m, so it cannot be used together with request "vcpu": remove the limit`,
	}, {
		name:     "two alternatives of one request are never beside each other",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "small", class: "dra.cpu", amount: "250m"}, {name: "large", class: "dra.cpu", amount: "500m"}}}},
	}, {
		name:     "a request on another driver's class is not beside",
		requests: []testRequest{{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "gpu", class: "gpu.example.com"}},
	}, {
		name:     "an entry the strict decoding refuses",
		requests: cpus("250m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: `{"apiVersion": "v1alpha1", "cpuConfig": {"limt": "500m"}}`}},
		wantErr:  `config entry #0: opaque config: strict decoding error: unknown field "cpuConfig.limt"`,
	}, {
		name:     "an entry reaching only another driver's request is not judged",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "250m"}, {name: "gpu", class: "gpu.example.com"}},
		entries:  []testEntry{{requests: []string{"gpu"}, params: `{"apiVersion": "v1alpha1", "cpuConfig": {"limt": "500m"}}`}},
	}, {
		name:     "another driver's entry is not judged",
		requests: cpus("250m"),
		entries:  []testEntry{{driver: "gpu.example.com", params: `{"limit": 0}`}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			kinds := tc.kinds
			if kinds == nil {
				kinds = fleetKinds
			}
			err := CheckClaim(testSpec(tc.requests, tc.entries...), testDriverName, byClass(kinds))
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestParseLimit(t *testing.T) {
	for _, tc := range []struct {
		written  string
		want     string
		wantMax  bool
		wantBad  bool
		wantMill int64
	}{
		{written: "max", wantMax: true},
		{written: "1", want: "1", wantMill: 1000},
		{written: "0.5", want: "500m", wantMill: 500},
		{written: "500u", want: "500u", wantMill: 1},
		{written: "0", wantBad: true},
		{written: "0m", wantBad: true},
		{written: "-1", wantBad: true},
		{written: "", wantBad: true},
		{written: "MAX", wantBad: true},
	} {
		t.Run(tc.written, func(t *testing.T) {
			quantity, isMax, ok := ParseLimit(v1alpha1.Limit(tc.written))
			require.Equal(t, !tc.wantBad, ok)
			require.Equal(t, tc.wantMax, isMax)
			if tc.want != "" {
				require.Equal(t, tc.want, quantity.String())
				require.Equal(t, tc.wantMill, quantity.MilliValue(), "a cap below a millicore is rounded up")
			}
		})
	}
}
