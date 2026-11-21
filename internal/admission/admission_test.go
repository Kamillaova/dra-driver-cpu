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
	ownedClass := func(class string) Kind {
		kind := owned("class " + class)
		kind.OwnedClass = class
		return kind
	}
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
		want: map[string]Kind{"dra.cpu": shared, "dra.cpu.vms": ownedClass("dra.cpu.vms"), "dra.cpu.infra": shared},
	}, {
		name: "every profile's partitions",
		cfg: driverconfig.Config{Profiles: map[string]driverconfig.Profile{
			"r7625": {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE}}},
			"spdk":  {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}, {Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE}}},
			"plain": {CPUPartitions: []driverconfig.CPUPartition{{Name: "default"}}},
		}},
		want: map[string]Kind{"dra.cpu": shared, "dra.cpu.vms": ownedClass("dra.cpu.vms"), "dra.cpu.spdk": ownedClass("dra.cpu.spdk")},
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

// onNode is kinds as a node judges a claim, naming itself.
func onNode(kinds map[string]Kind, node string) map[string]Kind {
	named := make(map[string]Kind, len(kinds))
	for class, kind := range kinds {
		kind.Node = node
		named[class] = kind
	}
	return named
}

func exclusive(value string) string {
	return `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": ` + value + `}}`
}

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
		wantErr:  `request "vmm-cpu" has limit of 1, so it cannot be used together with request "vcpu": use cgroupConfig.threaded, or remove the limit`,
	}, {
		name:     "an amount alone beside several requests",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "helper", class: "dra.cpu"}, {name: "io", class: "dra.cpu"}},
		wantErr:  `request "vmm-cpu" has limit of 250m, so it cannot be used together with requests "vcpu", "helper", "io": use cgroupConfig.threaded, or remove the limit`,
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
		wantErr: `subrequest "helper/shared" has limit of 500m, so it cannot be used together with request "vcpu": use cgroupConfig.threaded, or remove the limit`,
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
	}, {
		name:     "WA33: two entries giving a request different cpuConfig.exclusive",
		requests: cpus("2"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`true`)}, {requests: []string{"cpus"}, params: exclusive(`false`)}},
		wantErr:  `request "cpus": cpuConfig.exclusive set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "WA45: two entries giving a request the same cpuConfig.exclusive",
		requests: cpus("2"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`true`)}, {params: exclusive(`true`)}},
		wantErr:  `request "cpus": cpuConfig.exclusive set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "WA35: a parent entry and an alternative's entry giving one alternative cpuConfig.exclusive",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "two", class: "dra.cpu", amount: "2"}, {name: "one", class: "dra.cpu", amount: "1"}}}},
		entries:  []testEntry{{requests: []string{"cpus/two"}, params: exclusive(`false`)}, {requests: []string{"cpus"}, params: exclusive(`true`)}},
		wantErr:  `subrequest "cpus/two": cpuConfig.exclusive set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:     "WA26: a numeric limit with the flag",
		requests: cpus("2"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true, "limit": "2"}}`}},
		wantErr:  "request \"cpus\": limit 2 on exclusive request (`cpuConfig.exclusive: true`); only `limit: max` is allowed",
	}, {
		name:     "limit max with the flag",
		requests: cpus("2"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true, "limit": "max"}}`}},
	}, {
		name:     "the flag's amount is no cap on several devices",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "1", count: 2}},
		entries:  []testEntry{{params: exclusive(`true`)}},
	}, {
		name:     "the flag's amount is no cap beside another request",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "2"}, {name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`true`)}},
	}, {
		name:     "a capped share beside a flagged request",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "2"}, {name: "vmm-cpu", class: "dra.cpu", amount: "250m"}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`true`)}},
		wantErr:  `request "vmm-cpu" has limit of 250m, so it cannot be used together with request "cpus": use cgroupConfig.threaded, or remove the limit`,
	}, {
		name:     "exclusive false on a shared class",
		requests: cpus("500m"),
		entries:  []testEntry{{params: exclusive(`false`)}},
	}, {
		name:     "exclusive true on an exclusive class has no effect",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{params: exclusive(`true`)}},
	}, {
		name:     "WA27: exclusive false on a named exclusive partition's class",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{requests: []string{"vcpu"}, params: exclusive(`false`)}},
		wantErr:  "request \"vcpu\": `cpuConfig.exclusive: false`, but class dra.cpu.vms gives exclusive CPUs only; drop cpuConfig.exclusive, or use a shared class such as dra.cpu",
	}, {
		name:     "exclusive false on an alternative of an exclusive class",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{{name: "shared", class: "dra.cpu", amount: "1"}, {name: "owned", class: "dra.cpu.vms", amount: "1"}}}},
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`false`)}},
		wantErr:  "subrequest \"cpus/owned\": `cpuConfig.exclusive: false`, but class dra.cpu.vms gives exclusive CPUs only; drop cpuConfig.exclusive, or use a shared class such as dra.cpu",
	}, {
		name:     "WB12: exclusive false on a node without partitions",
		kinds:    onNode(plainKinds, "worker-1"),
		requests: cpus("500m"),
		entries:  []testEntry{{requests: []string{"cpus"}, params: exclusive(`false`)}},
		wantErr:  "request \"cpus\": `cpuConfig.exclusive: false`, but node \"worker-1\" runs without partitions, where dra.cpu gives exclusive CPUs only; add `- name: default` to cpuPartitions, or drop cpuConfig.exclusive",
	}, {
		name:     "exclusive true on a node without partitions",
		kinds:    onNode(plainKinds, "worker-1"),
		requests: cpus("2"),
		entries:  []testEntry{{params: exclusive(`true`)}},
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

func threaded(block string) string {
	return `{"apiVersion": "v1alpha1", "cgroupConfig": {"threaded": ` + block + `}}`
}

func TestCheckClaimThreaded(t *testing.T) {
	vm := []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "vmm-cpu", class: "dra.cpu", amount: "250m"}}
	split := `{"enabled": true, "place": "vmm-cpu", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "vmm-cpu", "requests": ["vmm-cpu"]}]}`
	helper := func(shared, owned string) testRequest {
		return testRequest{name: "helper", alternatives: []testRequest{{name: "shared", class: "dra.cpu", amount: shared}, {name: owned, class: "dra.cpu.vms", amount: "2"}}}
	}
	mixed := testRequest{name: "helper", alternatives: []testRequest{{name: "cpu", class: "dra.cpu", amount: "250m"}, {name: "gpu", class: "gpu.example.com"}}}
	for _, tc := range []struct {
		name     string
		kindOf   func(ref, class string) (Kind, bool)
		requests []testRequest
		entries  []testEntry
		wantErr  string
	}{{
		name:     "the VM's split",
		requests: vm,
		entries:  []testEntry{{params: threaded(split)}},
	}, {
		name:     "an entry listing every request means one listing none",
		requests: vm,
		entries:  []testEntry{{requests: []string{"vcpu", "vmm-cpu"}, params: threaded(split)}},
	}, {
		name:     "one group per request without a groups list",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "vmm-cpu"}`)}},
	}, {
		name:     "WB9: a layout without enabled: true is inert, the claim judged unsplit",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"place": "vmm-cpu", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "vmm-cpu", "requests": ["vmm-cpu"]}]}`)}},
		wantErr:  `request "vmm-cpu" has limit of 250m, so it cannot be used together with request "vcpu": use cgroupConfig.threaded, or remove the limit`,
	}, {
		name:     "an all-zero block in a scoped entry is no entry",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "vmm-cpu", class: "dra.cpu", amount: "0"}},
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: threaded(`{}`)}},
	}, {
		name:     "WA1: two groups with one name",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "vmm-cpu", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "vcpu", "requests": ["vmm-cpu"]}]}`)}},
		wantErr:  `group "vcpu" appears twice in cgroupConfig.threaded.groups; group names must be unique`,
	}, {
		name:     "WA2: one request in two groups",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "b", "groups": [{"name": "a", "requests": ["vcpu", "vmm-cpu"]}, {"name": "b", "requests": ["vmm-cpu"]}]}`)}},
		wantErr:  `request "vmm-cpu": in groups "a" and "b"; a request belongs to one group`,
	}, {
		name:     "WA3: a groups list that leaves a request out",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "vmm-cpu", "groups": [{"name": "vmm-cpu", "requests": ["vmm-cpu"]}]}`)}},
		wantErr:  `request "vcpu": in no group of cgroupConfig.threaded.groups; add it to a group, or drop groups for one group per request`,
	}, {
		name:     "WA4: place names no group",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "vmm"}`)}},
		wantErr:  `cgroupConfig.threaded.place "vmm": no such group (groups: vcpu, vmm-cpu)`,
	}, {
		name:     "WA5: an empty groups list",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": []}`)}},
		wantErr:  "cgroupConfig.threaded.groups: empty; list the groups, or drop it for one group per request",
	}, {
		name:     "an empty groups list in an inert block",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}},
		entries:  []testEntry{{params: threaded(`{"groups": []}`)}},
		wantErr:  "cgroupConfig.threaded.groups: empty; list the groups, or drop it for one group per request",
	}, {
		name: "WA6: a group naming one alternative and no group reaching the other",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{
			{name: "two", class: "dra.cpu", count: 2}, {name: "one", class: "dra.cpu", count: 1}}}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "cpus", "requests": ["cpus/two"]}]}`)}},
		wantErr: `subrequest "cpus/one": in no group of cgroupConfig.threaded.groups; add it or its request "cpus" to a group, or drop groups for one group per request`,
	}, {
		name:     "WA8: cgroupConfig scoped to some of the claim's requests",
		requests: vm,
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: threaded(`{"enabled": true}`)}},
		wantErr:  `cgroupConfig: applies to the whole claim; drop "requests" from its config entry, or list every request (vcpu, vmm-cpu)`,
	}, {
		name:     "WA43: an inert block in a scoped entry",
		requests: vm,
		entries:  []testEntry{{requests: []string{"vmm-cpu"}, params: threaded(`{"place": "vmm-cpu"}`)}},
		wantErr:  `cgroupConfig: applies to the whole claim; drop "requests" from its config entry, or list every request (vcpu, vmm-cpu)`,
	}, {
		name: "WA21: a capped request sharing a group with owned CPUs",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"},
			{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "all", "requests": ["vcpu", "vmm-cpu"]}]}`)}},
		wantErr: `group "all": request "vmm-cpu" has limit of 250m, so it cannot be used together with request "vcpu": give it its own group`,
	}, {
		name: "a capped request beside several requests in a group",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"},
			{name: "vmm-cpu", class: "dra.cpu", amount: "250m"}, {name: "helper", class: "dra.cpu"}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "all", "requests": ["vcpu", "vmm-cpu", "helper"]}]}`)}},
		wantErr: `group "all": request "vmm-cpu" has limit of 250m, so it cannot be used together with requests "vcpu", "helper": give it its own group`,
	}, {
		name:     "a capped request on several devices in its own group",
		requests: []testRequest{{name: "cpus", class: "dra.cpu", amount: "1", count: 2}},
		entries:  []testEntry{{params: threaded(`{"enabled": true}`)}},
		wantErr:  "request \"cpus\" has limit of 1 with `count: 2`; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name:     "uncapped shares beside owned CPUs in a group",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "io", class: "dra.cpu"}},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "all", "requests": ["vcpu", "io"]}]}`)}},
	}, {
		name:     "WA31: a group name that is not a DNS label",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "VCpu", "requests": ["vcpu"]}, {"name": "vmm-cpu", "requests": ["vmm-cpu"]}]}`)}},
		wantErr:  `group "VCpu": not a DNS label; use lower-case letters, digits and "-"`,
	}, {
		name:     "a group with no requests",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "vcpu", "requests": []}]}`)}},
		wantErr:  `group "vcpu" names no request; name its requests, or drop the group`,
	}, {
		name:     "a group naming no request of the claim",
		requests: vm,
		entries:  []testEntry{{params: threaded(`{"groups": [{"name": "vcpu", "requests": ["vcpus"]}]}`)}},
		wantErr:  `group "vcpu": the claim has no request "vcpus"`,
	}, {
		name:     "WA38: a mixed request's dra.cpu alternative in no group",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, mixed},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "gpu", "requests": ["helper/gpu"]}]}`)}},
		wantErr:  `subrequest "helper/cpu": in no group of cgroupConfig.threaded.groups; add it or its request "helper" to a group, or drop groups for one group per request`,
	}, {
		name: "WA39: place names a group that names no request whole",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"},
			{name: "helper", alternatives: []testRequest{{name: "cpu", class: "dra.cpu"}, {name: "gpu", class: "gpu.example.com"}}}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "place": "helper", "groups": [{"name": "vcpu", "requests": ["vcpu", "helper/cpu"]}, {"name": "helper", "requests": ["helper/gpu"]}]}`)}},
		wantErr: `cgroupConfig.threaded.place "helper": group "helper" names no request whole nor every alternative of one, and is not built where none of its alternatives is allocated; place a group that names a request`,
	}, {
		name:     "place on a group naming every alternative of a request",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, helper("0", "owned")},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "helper", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "helper", "requests": ["helper/shared", "helper/owned"]}]}`)}},
	}, {
		name: "WA40: a request and one of its own alternatives in two groups",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{
			{name: "two", class: "dra.cpu", count: 2}, {name: "one", class: "dra.cpu", count: 1}}}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "a", "requests": ["cpus"]}, {"name": "b", "requests": ["cpus/two"]}]}`)}},
		wantErr: `group "a" names request "cpus" and group "b" its alternative "cpus/two"; name the request or its alternatives, not both`,
	}, {
		name: "a request and one of its own alternatives in one group",
		requests: []testRequest{{name: "cpus", alternatives: []testRequest{
			{name: "two", class: "dra.cpu", count: 2}, {name: "one", class: "dra.cpu", count: 1}}}},
		entries: []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "a", "requests": ["cpus", "cpus/two"]}]}`)}},
		wantErr: `group "a" names request "cpus" and its alternative "cpus/two"; name the request or its alternatives, not both`,
	}, {
		name:     "WA42: a capped alternative in a group with another request",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, helper("500m", "owned")},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu", "helper"]}]}`)}},
		wantErr:  `group "vcpu": subrequest "helper/shared" has limit of 500m, so it cannot be used together with request "vcpu": give it its own group`,
	}, {
		name:     "WB26: one request's alternatives split across groups",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, helper("500m", "owned")},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu", "helper/owned"]}, {"name": "helper", "requests": ["helper/shared"]}]}`)}},
	}, {
		name:     "two alternatives of one request in one group are never beside each other",
		requests: []testRequest{helper("500m", "owned")},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "helper", "requests": ["helper"]}]}`)}},
	}, {
		name:     "WB22: a group naming another driver's request",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "gpu", class: "gpu.example.com"}},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "all", "requests": ["vcpu", "gpu"]}]}`)}},
	}, {
		name:     "WB27: an entry scoped to a mixed request reaches every dra.cpu result",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, mixed},
		entries:  []testEntry{{requests: []string{"vcpu", "helper"}, params: threaded(`{"enabled": true}`)}},
	}, {
		name:     "WB30: place on a group naming a mixed request whole passes the claim's check",
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, mixed},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "place": "helper", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "helper", "requests": ["helper"]}]}`)}},
	}, {
		name: "WB28: a request known by its allocation alone in no group",
		kindOf: func(ref, class string) (Kind, bool) {
			if class == "hand-written" {
				return Kind{Shared: true, FromAllocation: true}, true
			}
			kind, ok := fleetKinds[class]
			return kind, ok
		},
		requests: []testRequest{{name: "vcpu", class: "dra.cpu.vms", amount: "2"}, {name: "extra", class: "hand-written"}},
		entries:  []testEntry{{params: threaded(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu"]}]}`)}},
		wantErr:  `request "extra" is in no group; add it to cgroupConfig.threaded.groups`,
	}, {
		name:     "WA46: two entries carrying the same cgroupConfig",
		requests: vm,
		entries:  []testEntry{{params: threaded(split)}, {params: threaded(split)}},
		wantErr:  "cgroupConfig set by two config entries (#0 and #1); set it in one entry",
	}, {
		name:     "WA47: two entries carrying different cgroupConfig",
		requests: vm,
		entries:  []testEntry{{params: threaded(split)}, {requests: []string{"vcpu", "vmm-cpu"}, params: threaded(`{"enabled": true}`)}},
		wantErr:  "cgroupConfig set by two config entries (#0 and #1); set it in one entry",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			kindOf := tc.kindOf
			if kindOf == nil {
				kindOf = byClass(fleetKinds)
			}
			err := CheckClaim(testSpec(tc.requests, tc.entries...), testDriverName, kindOf)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestGroupOf(t *testing.T) {
	listed := v1alpha1.ThreadedConfig{Enabled: true, Groups: []v1alpha1.ThreadedGroup{
		{Name: "vcpu", Requests: []string{"vcpu", "helper/owned"}}, {Name: "helper", Requests: []string{"helper/shared"}}}}
	for _, tc := range []struct {
		block     v1alpha1.ThreadedConfig
		ref       string
		wantGroup string
		wantOK    bool
	}{
		{block: listed, ref: "vcpu", wantGroup: "vcpu", wantOK: true},
		{block: listed, ref: "helper/owned", wantGroup: "vcpu", wantOK: true},
		{block: listed, ref: "helper/shared", wantGroup: "helper", wantOK: true},
		{block: listed, ref: "io"},
		{block: v1alpha1.ThreadedConfig{Enabled: true}, ref: "helper/shared", wantGroup: "helper", wantOK: true},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			group, ok := GroupOf(tc.block, tc.ref)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantGroup, group)
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
