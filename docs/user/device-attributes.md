# Device Attributes and Selectors

Every CPU device the driver publishes carries topology attributes that claims can select on
with [CEL expressions](https://kubernetes.io/docs/reference/using-api/cel/).
This page is the attribute reference, with worked selector examples and sample
`ResourceSlice` objects for each device mode.

> [!NOTE]
> Attribute names and semantics are not yet a stable API: they may still change between
> driver minor releases while the project is pre-1.0.

## Attribute reference

Which attributes a device carries depends on the driver's device mode
(`cpuDeviceMode` in [Configuration](configuration.md)): `grouped` exposes one device per
CPU group. `individual` exposes one device per CPU, but is **deprecated**; see
[Migrating from individual mode](opaque-cpuset-overrides.md#migrate-from-individual-mode).

> [!IMPORTANT]
> The driver supports only cores with one or two logical CPUs. It refuses to start when
> a managed CPU belongs to a core with three or more logical CPUs.

### Grouped mode (default)

#### Supported attributes

| Attribute                                 | Type    | Description                                                                                                    |
| ----------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------- |
| `resource.kubernetes.io/numaNode`         | int     | Standard NUMA node of the group; published when grouping by NUMA node or uncore cache                          |
| `resource.kubernetes.io/pcieRoot`         | strings | PCIe roots local to the group's CPUs; needs `--expose-pcie-roots` and the `DRAListTypeAttributes` feature gate |
| `dra.cpu/socketID`                        | int     | CPU socket of the group; published when grouping by socket, NUMA node or uncore cache                          |
| `dra.cpu/cacheL3ID`                       | int     | Uncore (L3) cache of the group, by the id the kernel gives it; published when grouping by uncore cache         |
| `dra.cpu/numCPUs`                         | int     | Number of allocatable CPUs in the group                                                                        |
| `dra.cpu/smtEnabled`                      | bool    | Whether SMT/hyper-threading is enabled on the node                                                             |
| `dra.cpu/threadsPerCore`                  | int     | Logical CPUs on each physical core of the group; `0` when its cores differ, as on a hybrid CPU                 |
| `dra.cpu/fullPhysicalCPUsOnly`            | bool    | The driver's `fullPhysicalCPUsOnly` setting                                                                    |
| `dra.cpu/allocator`                       | string  | The driver's `allocator` setting, `cpumanager` or `external`                                                   |
| `dra.cpu/allowFractionalCapacityRequests` | bool    | The driver's `allowFractionalCapacityRequests` setting                                                         |
| `dra.cpu/partition`                       | string  | The CPU partition the group's CPUs belong to; `default` on a node without `cpuPartitions`                      |
| `dra.cpu/role`                            | string  | That partition's role, `default` or `exclusive`                                                                |

On a node with `cpuPartitions`, a group is published once for each partition that holds CPUs
in it, with that partition's CPUs alone, and the device of every partition but `default` is
named `<group device>-<partition>`, such as `cpudevnuma000-vms`. Its `dra.cpu/threadsPerCore`
counts the threads of each core inside the partition alone, and its `dra.cpu/smtEnabled` is
`false` only where that count is 1.

A partition of role `default` publishes shared devices: any number of claims hold one, each booking
an amount of its `dra.cpu/cpu` capacity, from `0`, which a request naming no amount books, up to the
device's CPU count, in whole CPUs or, with `allowFractionalCapacityRequests`, in millicores. The
claims run on the device's CPUs together, and no claim takes any of them for itself.

`dra.cpu/fullPhysicalCPUsOnly`, `dra.cpu/allocator` and `dra.cpu/allowFractionalCapacityRequests` let
a claim's selector keep it off the nodes that would refuse to prepare it; see
[Settings published as device attributes](configuration.md#settings-published-as-device-attributes).

#### External allocator attributes

With the external allocator in grouped mode, the driver also publishes these attributes for NUMA-node, uncore-cache, socket, and machine groups.

| Attribute           | Type   | Description                                                                                         |
| ------------------- | ------ | --------------------------------------------------------------------------------------------------- |
| `dra.cpu/cpuIDs`    | string | Linux cpuset representation of the allocatable logical CPUs in the group                            |
| `dra.cpu/smtLayout` | string | Node-wide logical-CPU sibling layout; intersect it with `dra.cpu/cpuIDs` for the group’s SMT layout |

See [SMT layout encoding](../dev/smt-layout.md) for the `dra.cpu/smtLayout` format.

For example, an external allocator might receive the following additional attributes for a
group containing CPUs 0 through 7, whose sibling offset is 4:

```yaml
dra.cpu/cpuIDs:
  string: 0-7
dra.cpu/smtLayout:
  string: 0-7>4
```

#### Compatibility / legacy attributes

These attributes are retained for compatibility with existing consumers and potential
cross-driver alignment. New consumers should use the supported attributes above instead.
These attributes will be removed in a future release.

| Attribute            | Type | Replaced by                       | Description                                                                                              |
| -------------------- | ---- | --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `dra.cpu/numaNodeID` | int  | `resource.kubernetes.io/numaNode` | Driver-specific NUMA node; published when grouping by NUMA node or uncore cache                          |
| `dra.net/numaNode`   | int  | `resource.kubernetes.io/numaNode` | Experimental cross-driver NUMA-alignment attribute; published when grouping by NUMA node or uncore cache |

### Individual mode (deprecated)

This is retained as a reference for existing deployments. For the supported replacement, see
[Migrating from individual mode](opaque-cpuset-overrides.md#migrate-from-individual-mode).

#### Supported attributes

| Attribute                                 | Type    | Description                                                                                           |
| ----------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------- |
| `resource.kubernetes.io/numaNode`         | int     | Standard NUMA node                                                                                    |
| `resource.kubernetes.io/pcieRoot`         | strings | PCIe roots local to the CPU; needs `--expose-pcie-roots` and the `DRAListTypeAttributes` feature gate |
| `dra.cpu/cpuID`                           | int     | Logical CPU ID                                                                                        |
| `dra.cpu/coreID`                          | int     | Physical core ID, shared by SMT siblings                                                              |
| `dra.cpu/coreType`                        | string  | `standard`, `p-core`, or `e-core`                                                                     |
| `dra.cpu/cacheL3ID`                       | int     | L3 (last-level/uncore) cache group                                                                    |
| `dra.cpu/socketID`                        | int     | CPU socket                                                                                            |
| `dra.cpu/smtEnabled`                      | bool    | Whether SMT/hyper-threading is enabled on the node                                                    |
| `dra.cpu/fullPhysicalCPUsOnly`            | bool    | The driver's `fullPhysicalCPUsOnly` setting, `false` in this mode                                     |
| `dra.cpu/allocator`                       | string  | The driver's `allocator` setting, `cpumanager` or `external`                                          |
| `dra.cpu/allowFractionalCapacityRequests` | bool    | The driver's `allowFractionalCapacityRequests` setting                                                |
| `dra.cpu/partition`                       | string  | `default`: this mode takes no `cpuPartitions`                                                         |
| `dra.cpu/role`                            | string  | `default`                                                                                             |

#### Compatibility / legacy attributes

These attributes are retained for compatibility. New consumers should use the supported
attributes above instead. They may be removed in a future release.

| Attribute            | Type | Replaced by                       | Description                                                                                   |
| -------------------- | ---- | --------------------------------- | --------------------------------------------------------------------------------------------- |
| `dra.cpu/numaNodeID` | int  | `resource.kubernetes.io/numaNode` | Driver-specific NUMA node; published only when grouping by NUMA node                          |
| `dra.net/numaNode`   | int  | `resource.kubernetes.io/numaNode` | Experimental cross-driver NUMA-alignment attribute; published only when grouping by NUMA node |

### Prepared-device metadata

When a grouped device is prepared, the driver copies its published attributes into the device
metadata exposed to the container. It also adds the following metadata-only attributes:

| Attribute                  | Type   | Description                                                                                                                                                                                                                     |
| -------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dra.cpu/allocatedNumCPUs` | int    | **Deprecated:** use `dra.cpu/cpuRequest`. Number of CPUs allocated to this claim from the grouped device, written for each request that holds exclusive CPUs when the claim consumes `dra.cpu/cpu` capacity                     |
| `dra.cpu/cpuRequest`       | string | The CPU amount booked for the request on this device, as a quantity such as `2` or `1500m`, when the claim consumes `dra.cpu/cpu` capacity                                                                                      |
| `dra.cpu/cpuLimit`         | string | The request's CPU limit as a quantity, or `max` where the request has none; a shared request's is its amount, `max` where it books nothing, and a request that holds exclusive CPUs has none; written with `dra.cpu/cpuRequest` |
| `dra.cpu/cpuset`           | string | The CPUs the request holds on this device, in cpuset list form such as `0-1,4-5`, for a request that holds exclusive CPUs, in either device mode                                                                                |

These attributes are not published in a `ResourceSlice` and cannot be used to select a device. A
container reads them in the request's [device metadata file](device-metadata.md).

## Versioned attributes

Some attributes contain a compact, driver-defined encoding instead of a Kubernetes-standard
value. We cannot promise that a encoding introduced by this driver is correct and complete on its
first release, or that it will not need a bug fix or extension.
We therefore reserve the option to version those attribute names should update to their format be needed,
giving consumers a basis for a smooth upgrade instead of forcing every consumer to change at once.
For the sake of practicality, `V1` is implicit and never set explicitly.
In particular, `dra.cpu/smtLayout` is the first version of the SMT sibling-map encoding; should
a new iteration be needed, it will be published as `dra.cpu/smtLayoutV2`.

The suffix is a compact semantic version. Trailing `.0` components are omitted, should we
need minor or even micro versions:

| Attribute-name suffix | Semantic version |
| --------------------- | ---------------- |
| (omitted)             | `v1.0.0`         |
| `V2_1`                | `v2.1.0`         |
| `V3_1_2`              | `v3.1.2`         |

The DRA attribute identifier permits letters, digits, and `_`, but not `.` or `-`. Therefore the
periods in the conceptual versions `V2.1` and `V3.1.2` are represented by underscores in the
actual attribute names: `dra.cpu/smtMapV2_1` and `dra.cpu/smtMapV3_1_2`.

When introducing an incompatible encoding, the driver can publish both the old and new attributes
(for example, `dra.cpu/smtLayout` and `dra.cpu/smtLayoutV2`) for a migration period.
Consumers should select the highest version that they understand and ignore newer versions.
Once published, the meaning of a versioned attribute is immutable.

## Example ResourceSlices

Here's how the `ResourceSlice` objects might look for each mode.

### Grouped mode (default; grouping by NUMA node)

CPUs are grouped, and the device entry shows consumable capacity.

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceSlice
metadata:
  name: 00000-dra.cpu-dra-driver-cpu-worker-tp869
  # ... other metadata
spec:
  driver: dra.cpu
  nodeName: dra-driver-cpu-worker
  pool:
    generation: 1
    name: dra-driver-cpu-worker
    resourceSliceCount: 1
  devices:
  - allowMultipleAllocations: true
    attributes:
      dra.cpu/smtEnabled:
        bool: true
      dra.cpu/numCPUs:
        int: 64
      dra.cpu/threadsPerCore:
        int: 2
      dra.cpu/fullPhysicalCPUsOnly:
        bool: false
      dra.cpu/allocator:
        string: cpumanager
      dra.cpu/allowFractionalCapacityRequests:
        bool: false
      dra.cpu/partition:
        string: default
      dra.cpu/role:
        string: default
      resource.kubernetes.io/numaNode:
        int: 0
      dra.cpu/socketID:
        int: 0
      dra.net/numaNode:
        int: 0
      dra.cpu/numaNodeID:
        int: 0
      # Only populated if the driver is run with --expose-pcie-roots=true
      resource.kubernetes.io/pcieRoot:
        strings:
        - pci0000:00
        - pci0000:10
    capacity:
      dra.cpu/cpu:
        value: "64"
    name: cpudevnuma000
  - allowMultipleAllocations: true
    attributes:
      dra.cpu/smtEnabled:
        bool: true
      dra.cpu/numCPUs:
        int: 64
      dra.cpu/threadsPerCore:
        int: 2
      dra.cpu/fullPhysicalCPUsOnly:
        bool: false
      dra.cpu/allocator:
        string: cpumanager
      dra.cpu/allowFractionalCapacityRequests:
        bool: false
      dra.cpu/partition:
        string: default
      dra.cpu/role:
        string: default
      resource.kubernetes.io/numaNode:
        int: 1
      dra.cpu/socketID:
        int: 0
      dra.net/numaNode:
        int: 1
      dra.cpu/numaNodeID:
        int: 1
      # Only populated if the driver is run with --expose-pcie-roots=true
      resource.kubernetes.io/pcieRoot:
        strings:
        - pci0000:40
        - pci0000:50
    capacity:
      dra.cpu/cpu:
        value: "64"
    name: cpudevnuma001
```

### Individual mode (deprecated)

In this [mode](opaque-cpuset-overrides.md#migrate-from-individual-mode),
each CPU is listed as a separate device with detailed attributes.

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceSlice
metadata:
  name: dra-driver-cpu-worker-dra.cpu-qskwf
  # ... other metadata
spec:
  driver: dra.cpu
  nodeName: dra-driver-cpu-worker
  pool:
    generation: 1
    name: dra-driver-cpu-worker
    resourceSliceCount: 1
  devices:
  - attributes:
      dra.cpu/cacheL3ID:
        int: 0
      dra.cpu/coreID:
        int: 1
      dra.cpu/coreType:
        string: standard
      dra.cpu/cpuID:
        int: 1
      resource.kubernetes.io/numaNode:
        int: 0
      dra.cpu/smtEnabled:
        bool: true
      dra.cpu/fullPhysicalCPUsOnly:
        bool: false
      dra.cpu/allocator:
        string: cpumanager
      dra.cpu/allowFractionalCapacityRequests:
        bool: false
      dra.cpu/partition:
        string: default
      dra.cpu/role:
        string: default
      dra.cpu/socketID:
        int: 0
      dra.net/numaNode:
        int: 0
      dra.cpu/numaNodeID:
        int: 0
      # Only populated if the driver is run with --expose-pcie-roots=true
      resource.kubernetes.io/pcieRoot:
        strings:
        - pci0000:00
    name: cpudev000
  - attributes:
      dra.cpu/cacheL3ID:
        int: 0
      dra.cpu/coreID:
        int: 1
      dra.cpu/coreType:
        string: standard
      dra.cpu/cpuID:
        int: 33
      resource.kubernetes.io/numaNode:
        int: 0
      dra.cpu/smtEnabled:
        bool: true
      dra.cpu/fullPhysicalCPUsOnly:
        bool: false
      dra.cpu/allocator:
        string: cpumanager
      dra.cpu/allowFractionalCapacityRequests:
        bool: false
      dra.cpu/partition:
        string: default
      dra.cpu/role:
        string: default
      dra.cpu/socketID:
        int: 0
      dra.net/numaNode:
        int: 0
      dra.cpu/numaNodeID:
        int: 0
      # Only populated if the driver is run with --expose-pcie-roots=true
      resource.kubernetes.io/pcieRoot:
        strings:
        - pci0000:00
    name: cpudev001
  # ... other CPU devices
```

### With node allocatable mapping

When the driver runs with `publishNodeAllocatableResourceMapping: true` (requires the
`DRANodeAllocatableResources` feature gate, alpha in 1.37+), every device additionally
carries a `nodeAllocatableResources` entry translating its DRA allocation into node
allocatable `cpu`. On a node that uses `cpuPartitions` only the devices of the `default`
entry carry it, since the kubelet's reservation already holds the CPUs of every other
partition.

Grouped mode maps the consumed `dra.cpu/cpu` capacity 1:1:

```yaml
  devices:
  - allowMultipleAllocations: true
    capacity:
      dra.cpu/cpu:
        value: "64"
    name: cpudevnuma000
    nodeAllocatableResources:
      cpu:
        mapping:
          capacityKey: dra.cpu/cpu
          capacityMultiplier: "1"
```

For reference and completeness, the [`individual`](opaque-cpuset-overrides.md#migrate-from-individual-mode)
mode maps each device to one CPU.

```yaml
  devices:
  - name: cpudev000
    nodeAllocatableResources:
      cpu:
        mapping:
          deviceMultiplier: "1"
```

### With whole physical cores

When the driver runs with `fullPhysicalCPUsOnly: true`, a group leaves out every core whose
logical CPUs `reservedCPUs` splits. A group whose cores all have two logical CPUs also carries a
request policy in whole cores, so the scheduler rounds a request up to whole cores, for example 3
CPUs to 4:

```yaml
  devices:
  - allowMultipleAllocations: true
    attributes:
      dra.cpu/threadsPerCore:
        int: 2
    capacity:
      dra.cpu/cpu:
        value: "64"
        requestPolicy:
          default: "64"
          validRange:
            min: "2"
            step: "2"
    name: cpudevnuma000
```

A group of a single core gets `min` without `step`, since the API server refuses a step that does
not fit in the capacity. A group whose cores have one logical CPU, or differ, carries no request
policy.

## Selecting CPUs based on properties with CEL

A selector is a [CEL](https://kubernetes.io/docs/reference/using-api/cel/) expression over
the attributes above; the scheduler only allocates devices for which every selector is true.

In the default `grouped` mode, CPUs are requested as `dra.cpu/cpu` capacity from a group
device, and selectors pick the group. A complete claim for 8 CPUs from NUMA node 0:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: cpus-on-numa0
spec:
  devices:
    requests:
    - name: cpus
      exactly:
        deviceClassName: dra.cpu
        capacity:
          requests:
            dra.cpu/cpu: "8"
        selectors:
        - cel:
            expression: device.attributes["resource.kubernetes.io"].numaNode == 0
```

In the [`individual`](opaque-cpuset-overrides.md#migrate-from-individual-mode)
mode, each CPU is its own device, so claims request a `count` of
devices and selectors pick individual CPUs.
A complete legacy claim for 4 performance cores:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: performance-cores
spec:
  devices:
    requests:
    - name: cpus
      exactly:
        deviceClassName: dra.cpu
        count: 4
        selectors:
        - cel:
            expression: device.attributes["dra.cpu"].coreType == "p-core"
```

Any supported attribute works the same way — for example, use
`device.attributes["dra.cpu"].smtEnabled == false` to avoid nodes with SMT/hyper-threading enabled
(for example, for side-channel isolation).

Selectors filter each request independently; to make *multiple* requests land on matching
topology, add a
[`matchAttribute`](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
constraint. A complete claim requesting two CPU sets that must share a socket:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: same-socket-cpus
spec:
  devices:
    requests:
    - name: cpus-a
      exactly:
        deviceClassName: dra.cpu
        capacity:
          requests:
            dra.cpu/cpu: "4"
    - name: cpus-b
      exactly:
        deviceClassName: dra.cpu
        capacity:
          requests:
            dra.cpu/cpu: "4"
    constraints:
    - requests: ["cpus-a", "cpus-b"]
      matchAttribute: dra.cpu/socketID
```

The inverse is `distinctAttribute`: every request must get a *different* value. A complete
claim spreading two CPU sets across two different NUMA nodes — without hardcoding which
nodes, so the same claim works on any machine:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: numa-spread-cpus
spec:
  devices:
    requests:
    - name: cpus-a
      exactly:
        deviceClassName: dra.cpu
        capacity:
          requests:
            dra.cpu/cpu: "8"
    - name: cpus-b
      exactly:
        deviceClassName: dra.cpu
        capacity:
          requests:
            dra.cpu/cpu: "8"
    constraints:
    - requests: ["cpus-a", "cpus-b"]
      distinctAttribute: resource.kubernetes.io/numaNode
```

`distinctAttribute` is gated by `DRAConsumableCapacity` — the same feature gate the default
`grouped` mode uses, enabled by default from Kubernetes 1.36.

Similarly, for equal-sized slices you can use a single request with `count` > 1.
This repeats the same per-result capacity request multiple times. By itself,
`count` does not guarantee spreading, but combined with `distinctAttribute` it
can force the scheduler to place those results on different devices:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: numa-spread-cpus
spec:
  devices:
    requests:
    - name: cpus-multi
      exactly:
        deviceClassName: dra.cpu
        count: 2
        capacity:
          requests:
            dra.cpu/cpu: "8"
    constraints:
    - requests: ["cpus-multi"]
      distinctAttribute: resource.kubernetes.io/numaNode
```

For a longer discussion of the trade-offs of using `count` +
`distinctAttribute` for NUMA spreading, see
[Feature Support](feature-support.md#distributing-cpus-across-numa-nodes).

**NOTE**: An important point to stress is the role of the `distinctAttribute` constraint.
In grouped mode, all the exposed devices support multiple allocations. Therefore,
without the constraint, the scheduler can pick the same device multiple times
to fulfil the allocation request, if that device has enough remaining capacity.
In turn, the driver supports this request shape and will honor the request,
because it is valid and legal.
