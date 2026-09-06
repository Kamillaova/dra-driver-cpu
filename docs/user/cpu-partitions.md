# CPU Partitions

A node's cores are not always interchangeable. Some must stay away from every workload, some carry
the pods that hold no claim, some belong to one daemon that pins its threads by CPU id, and some are
what virtual machines are placed on. `cpuPartitions` is where that is written down, once, and the
driver publishes, allocates and protects each set accordingly.

The field reference is in [Configuration](configuration.md#driverconfig-sub-fields); this page is the
model, what the platform has to do for it, and how to change it on a live node.

## The model

A partition is a named set of whole cores on one node, with one role and one expectation about how
many threads per core the platform leaves online.

| Role | What runs there |
| --- | --- |
| `reserved` | Nothing. No device is published from it and no pod is pinned to it, which is what `reservedCPUs` means. |
| `default` | Devices are published, and containers holding no claim run on whatever is left unclaimed. |
| `shared` | A pool a workload reaches by claiming it. |
| `exclusive` | Devices are published, and a container without a claim never runs there. |

The CPUs no partition names form the implicit partition `default`, which always exists, carries no
taint, and is where a claim that names no partition lands. Declaring a partition called `default` is
an error for that reason. An implicit partition left with no CPUs is allowed and logged: a node whose
cores are all described has nowhere to put a pod that asked for nothing.

Every declared partition's devices carry the `NoSchedule` device taint `dra.cpu/partition=<name>`,
and the chart renders a `DeviceClass` named `dra.cpu-<name>` for each of them. A claim reaches a
partition by naming that class and tolerating the taint:

```yaml
spec:
  devices:
    requests:
      - name: cores
        exactly:
          deviceClassName: dra.cpu-dataplane
          capacity:
            requests:
              dra.cpu/cpu: "4"
          tolerations:
            - key: dra.cpu/partition
              operator: Equal
              value: dataplane
              effect: NoSchedule
```

The class is the fence a template chooses with; the taint is what stops a permissive class written
later from handing those CPUs to a claim that never asked for them. Tolerate the key with
`operator: Equal` and a value: an `Exists` toleration with an empty key tolerates every taint there
is, including the ones a future release adds.

## Claiming a pool

A `shared` partition is published as one device per NUMA node it touches, and every claim that asks
for one holds it at the same time: what a claim gets is the device's whole CPU set, not a slice of
it. The amount a request names is an admission bound on how much work a template may put there, in
CPU units, down to a tenth of a CPU:

```yaml
      - name: helpers
        exactly:
          deviceClassName: dra.cpu-helpers
          capacity:
            requests:
              dra.cpu/cpu: "500m"
          tolerations:
            - key: dra.cpu/partition
              operator: Equal
              value: helpers
              effect: NoSchedule
```

Ask for it in the same claim as the exclusive request rather than in a claim of its own: a container
holding two claims is pinned to their union and nothing inside it can tell which CPUs came from
which. A request that names no amount takes the smallest share rather than the whole pool, so a
template that forgets the capacity cannot swallow what everything else is meant to share.

A pool's CPUs never move, so the driver names them in that request's device metadata file, mounted
at `/var/run/kubernetes.io/dra-device-attributes/<claim>/<request>/metadata.json` beside the
partition and role. A workload reads its pool from there; it still reads its exclusive CPUs from the
kernel, because those may change while it runs. Nothing is appended to a container that did not ask:
a container holding claims runs on its claims' CPUs and nothing else, and a container holding none
runs on the unclaimed CPUs of the `default` partitions.

Pool devices publish no node-allocatable mapping. Their CPUs already count once for the containers
that hold no claim, and mapping them would count the same cores against the node twice.

## What the machine has to provide

The driver verifies and the platform configures. It never changes CPU hotplug state itself.

**Offlining SMT siblings.** A partition that declares `smt: false` expects one online thread per
core. Nothing in the driver can produce that: the platform takes the siblings offline, and the
driver checks that it did. On Talos the keys are under `machine.sysfs`, dotted and relative to
`/sys`:

```yaml
machine:
  sysfs:
    devices.system.cpu.cpu136.online: "0"
    devices.system.cpu.cpu137.online: "0"
```

When the check fails, the driver names the exact keys that would satisfy it, in its log and in a
`CPUPartitionDegraded` event on the node, and that partition alone publishes no devices. The rest of
the node keeps serving: a partition whose machine configuration has not landed yet is not a reason to
take the node's other workloads down. `dra_cpu_partition_verified{partition="..."}` is `0` while that
holds.

**Kernel arguments.** These are companions the driver cannot provide, and each buys something
different. Set them in `machine.install.extraKernelArgs`, which needs a reboot.

- `irqaffinity=<housekeeping cpus>` — interrupts affine to a CPU that goes offline are moved to
  whatever is left in their mask, so without this they can land on the very cores a partition was
  carved out to keep quiet.
- `isolcpus=managed_irq,domain,<partition cpus>` — keeps managed NVMe and NIC queues off those cores
  and takes them out of the scheduler's load balancing, which matters while the driver is restarting
  and nothing is pinning anything.
- `kernel.watchdog_cpumask` without those cores — the per-CPU lockup detector's timer stops running
  on them.
- `nohz_full` is silently ignored on a kernel built without `CONFIG_NO_HZ_FULL`, so check the kernel
  before counting on a tickless core.

None of this is required. A partition works without any of it; these reduce what else the kernel puts
on those cores.

**The kubelet.** `cpuManagerPolicy: none`, as everywhere with this driver. With the
`DRANodeAllocatableResources` feature gate on and
[`publishNodeAllocatableResourceMapping`](configuration.md#driverconfig-sub-fields) set, the CPUs a
claim holds are subtracted from node allocatable as claims arrive, and `kubeReserved.cpu` should
equal the reserved partition plus every pool: those are the CPUs no pod without a claim can use and
no claim ever subtracts. With the gate off, the pods that hold claims mirror their claims' CPUs into
their own requests instead — see
[Workload Configuration Requirements](workload-requirements.md).

The kubelet reads `capacity.cpu` once, at start. Offlining siblings under a running kubelet therefore
leaves the node advertising CPUs that no longer exist until it restarts.

## Changing the partitions of a running node

The partition list is the ground truth under every current placement, so it is read once at startup
rather than watched. Changing it takes a driver restart on that node, and changing the machine
configuration it depends on takes more:

1. Apply the configuration change (the partition list, the `machine.sysfs` keys, or both).
2. If CPU hotplug state changed, restart the kubelet, or its capacity keeps counting CPUs that are
   now offline.
3. Restart the driver's DaemonSet pod on that node. It reads the online set and the partitions at
   start.
4. Check the node's events and `dra_cpu_partition_verified` before placing anything that depends on
   the new layout.

Prefer a reboot where the node can take one: it puts the three steps in the one order that is always
correct, and the driver's own startup is the last of them.

A claim that was already running when the list changed is left alone. Its CPUs may no longer sit
inside any partition, which the driver logs and counts as `dra_cpu_misplaced_claims_total` on the
next `Synchronize`; stopping a running workload to enforce a description that changed under it is not
a decision the driver makes on its own.

## An example layout

A dual-socket node of 128 cores and 256 threads, siblings `n` and `n+128`, with a dataplane daemon
that wants whole quiet cores:

```yaml
driverConfig:
  cpuDeviceMode: grouped
  fullPhysicalCPUsOnly: true
  cpuPartitions:
    - name: system
      role: reserved
      cpus: "0,128"
    - name: helpers
      role: shared
      cpus: "4-7,132-135"
    - name: dataplane
      role: exclusive
      cpus: "8-15"
      smt: false
    - name: vm
      role: exclusive
      cpus: "16-127,144-255"
```

CPUs `1-3` and `129-131` are named by nothing, so they are the implicit partition where the pods
holding no claim run. The dataplane partition names one thread per core because the other eight
threads, `136-143`, are offline: to the driver, and to the kernel's own topology, they do not exist.

Which cores to give up is a decision about the fleet, not about the driver: no core in `reserved` or
in a pool is ever given to a claim exclusively, and every uncore cache one of them touches is a cache
no whole-cache claim can ever have.
