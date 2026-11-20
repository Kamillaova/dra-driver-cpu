# CPU Partitions

`cpuPartitions` splits a node's CPUs into named partitions. Each partition has a role, which decides
which devices it publishes and who runs on its CPUs. A node uses partitions only when its list holds
the entry `- name: default`. The field itself is described in
[Configuration](configuration.md#driverconfig-sub-fields).

## What `dra.cpu` gives

- Without partitions, with `cpuPartitions` left out, `null` or `[]`, the driver works as upstream's:
  `dra.cpu` gives exclusive CPUs taken out of the shared pool, the CPUs that containers without
  claims run on. The shared pool shrinks while a claim holds them and grows back after.
- With partitions, `dra.cpu` gives a share of the `default` partition's CPUs: any number of claims
  hold one of its devices, each booking an amount, and their containers run on the device's CPUs
  together with the containers without claims. Exclusive CPUs come only from the `exclusive`
  partitions, each through its own class, `dra.cpu.<name>`. Nothing takes CPUs of the `default`
  partition for itself, so the CPUs that containers without claims run on do not shrink.

The least configuration that uses partitions is `cpuPartitions: [{name: default}]`: the whole node
is then one `default` partition of shared devices.

A node without partitions keeps upstream's mode, so that a configuration valid upstream works as it
does upstream. One mode for both kinds of node is possible only with another kind of device on nodes
without partitions: there, an amount booked on `dra.cpu` is that many CPUs for the claim alone, and
on a shared device it is a share of CPUs that its holders run on together.

## Roles

| Role        | Publishes                                                                                        | Who runs on its CPUs                                                                                                                       |
| ----------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `default`   | Shared devices, one per group of `groupBy`; a request on them is a share                         | The `default` entry: containers without claims, and the claims on its devices. A partition of another name with this role: its claims only |
| `exclusive` | Owned devices, one per group of `groupBy`; a request on them takes CPUs of the device for itself | The containers of the claims that hold those CPUs                                                                                          |
| `reserved`  | Nothing                                                                                          | Nobody                                                                                                                                     |

Every partition but `default` names its role in `role` and its CPUs in `cpuset`. Its devices are
named `<group device>-<partition>`, such as `cpudevnuma000-vms`, and carry the partition's name and
role in `dra.cpu/partition` and `dra.cpu/role`; see
[Device Attributes](device-attributes.md#supported-attributes). Where
`publishNodeAllocatableResourceMapping` is on, only the `default` entry's devices carry the
node-allocatable mapping; see [Workload Configuration Requirements](workload-requirements.md).

## The `default` entry

The CPU numbers below are those of a node whose core N has the threads N and N+128.

```yaml
cpuPartitions:
  - name: default
  - name: vms
    role: exclusive
    cpuset: "16-127,144-255"
  - name: system
    role: reserved
    cpuset: "0,128"
```

- The entry takes a `name` and, optionally, `smt`. Its role is always `default`, and its CPUs are
  filled in: the online threads of every core that no other partition names, a core being named when
  any of its threads is listed. Writing `role` or `cpuset` on it is refused when the configuration
  loads, with `partition "default": role is always default; remove it` or
  `partition "default": cpuset is filled in automatically; remove it`.
- Its devices carry no suffix and its class is `dra.cpu`: the names a node without partitions
  publishes.
- Partitions are used only where the list holds the entry. `[]`, `null` and the key left out all mean
  that partitions are not used. A list that names partitions without the entry is refused when the
  configuration loads, and fails `helm install` and `helm upgrade` at render, with:

```text
cpuPartitions names partitions but no partition "default": add `- name: default` to cpuPartitions to use partitions
```

The same `reserved` partition, refused without the entry and used with it:

```yaml
# Refused: names a partition, but not default.
cpuPartitions:
  - name: system
    role: reserved
    cpuset: "0,128"
```

```yaml
# Uses partitions: the default partition is every online CPU but 0 and 128.
cpuPartitions:
  - name: default
  - name: system
    role: reserved
    cpuset: "0,128"
```

Under [profiles](configuration.md#per-node-profiles), every profile's list holds the entry, and a
node whose cores are all alike names a declared profile that holds only `- name: default`.

## Preparing the machine

A partition's `smt` says how many threads per core the platform leaves online on the partition's
cores: `true`, or left out, as many as the hardware has; `false`, one; a number, at most that many.
The driver checks it and never takes a CPU offline itself. Take the extra threads offline at boot,
through the platform's configuration. What the driver does with CPUs that go offline or online while
it runs is undefined.

A partition holds whole cores. Under `smt: true`, or with `smt` left out, it lists every thread of
each of its cores; under `false` or a number, at most that many threads of each core. The driver
refuses to start on a list that breaks this, that names CPUs the node does not have or that are
offline, or whose topology the kernel does not report, for example with:

```text
partition "vms" lists CPUs 1-2 without their sibling threads 5-6: list every thread of each core
partitions "a" and "b" split one core between them (CPU 1 in "a", CPU 5 in "b"): put both threads in one partition
partition "vms" lists CPUs 1,5 of one core under `smt: false`: list only the first thread of each core
partition "vms" lists offline CPUs 5: bring them online, or remove them from its cpuset
```

A thread that a partition's `smt` expects offline, but that is still online, stays in that partition
without being listed. It runs nothing, containers without claims included, it belongs to no device,
and it serves no other partition either, while the kubelet reservation counts it. Meanwhile the
partition is degraded and publishes no devices; see
[A CPU Partition Whose SMT the Kernel Contradicts](troubleshooting.md#a-cpu-partition-whose-smt-the-kernel-contradicts).
This is a known gap: until such threads are offline, the node reserves them and runs nothing on
them.

The `default` entry takes `smt` too. Under `false` or a number its CPUs are the lowest that many
online threads of each of its cores, and the other online threads of those cores are kept as above,
running nothing until they are offline. While it is degraded, containers without claims keep running
on its CPUs.

## Device classes

The chart renders `dra.cpu` for the `default` partition, and `dra.cpu.<name>` for every other
partition that is not `reserved`, across the fleet-wide list and every profile; see
[Installing with Helm](installation.md#installing-with-helm). Each class selects its partition's
devices by `dra.cpu/partition`, and that selector is the only thing that keeps a claim in its
partition: no device carries a partition taint. A class written by hand must select on
`dra.cpu/partition` too. A class that selects only `device.driver == "dra.cpu"` reaches every
partition's devices, those of the `exclusive` partitions included. The chart's class for `vms`:

```yaml
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: dra.cpu.vms
spec:
  selectors:
    - cel:
        expression: device.driver == "dra.cpu" && has(device.attributes["dra.cpu"].partition) && device.attributes["dra.cpu"].partition == "vms"
```

`has()` keeps a device published without the attribute, by a driver release without partitions,
from failing the claim's allocation: `dra.cpu` matches such a device as the `default` partition's.

## Claims through extended resources

From Kubernetes 1.36, where the `DRAExtendedResource` feature gate is on by default, every
DeviceClass is also the extended resource `deviceclass.resource.kubernetes.io/<class>`, whether or
not the class sets `extendedResourceName`. A container can ask for a class's devices in its plain
resources, and the scheduler then creates the pod's claim itself:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-ext
spec:
  containers:
    - name: app
      image: registry.k8s.io/pause:3.10
      resources:
        requests:
          deviceclass.resource.kubernetes.io/dra.cpu.vms: "2"
        limits:
          deviceclass.resource.kubernetes.io/dra.cpu.vms: "2"
```

The claim the scheduler creates is owned by the pod and holds one request for each container and
resource, with no configuration:

```yaml
spec:
  devices:
    requests:
      - name: container-0-request-0
        exactly:
          deviceClassName: dra.cpu.vms
          allocationMode: ExactCount
          count: 2
```

- The number counts devices, not CPUs: `2` is two whole `vms` devices, since a request that names no
  amount takes an owned device whole.
- On a node with partitions, `dra.cpu`'s shared device is booked at nothing, so such a container
  runs on the shared CPUs uncapped.
- A container uses a claim whole. Where a second container of the pod asks for the same resource,
  each container holds only part of the generated claim, and both are refused at creation. Ask for it
  from one container of a pod.

## Quotas

ResourceQuota bounds `dra.cpu` claims in devices and in number, never in CPUs: a shared device
counts as one device whatever amount a claim books on it. In a tenant namespace, bound the claims
written directly per class with `<class>.deviceclass.resource.k8s.io/devices` and in number with
`count/resourceclaims.resource.k8s.io`, and the extended-resource route with
`requests.deviceclass.resource.kubernetes.io/<class>`:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: cpu-claims
  namespace: tenant-a
spec:
  hard:
    count/resourceclaims.resource.k8s.io: "20"
    dra.cpu.vms.deviceclass.resource.k8s.io/devices: "4"
    requests.deviceclass.resource.kubernetes.io/dra.cpu.vms: "4"
```

## Capacity of a node with partitions

With the [kubelet reservation](#the-kubelet-reservation) set, a node's allocatable CPU is its
`default` partition's CPUs less what the node reserves beyond the partitions. Everything that runs
there shares it: the CPU requests of the pods without claims, DaemonSet pods and a RuntimeClass's
`overhead` included, and the amounts booked on the `default` partition's devices, which count
against the node's `cpu` through the node-allocatable mapping or through the claim containers'
requests; see [Workload Configuration Requirements](workload-requirements.md). So the size of the
`default` partition caps how many claim pods holding shares, such as VMs, a node takes.

## A node whose `default` partition holds no CPU

When the other partitions name every core, the `default` partition is empty: it publishes no device,
and no CPU is left for containers without claims. The driver refuses each of them at creation,
whether or not a claim is prepared on the node, with:

```text
container "app": no CPU claim, and node "worker-a" (profile "r7625") leaves no CPUs for containers without one; add a claim on an exclusive partition, or run it elsewhere
```

The profile is named only where the configuration declares profiles. A pod runs on such a node only
if every container, init containers included, holds a claim. With the kubelet reservation met, the
node's allocatable CPU is 0, so the scheduler places no pod that requests CPU there, node agents such
as the CNI's and kube-proxy included, and no static pod can hold a claim, so no control plane runs
there. The driver's own pod runs there only if it requests no CPU, `resources.requests.cpu: "0"` in
the chart's values, whose default is `100m`; its container is created before the driver connects to
the container runtime, so it is not refused. Software that such a node needs runs outside pods, as
system services. A node that runs node agents as pods keeps a small `default` partition.

## The kubelet reservation

On a node whose partitions keep CPUs from pods without claims (exclusive, reserved or named
default-role partitions, or threads a partition's `smt` leaves online), the kubelet must reserve
every online CPU outside the `default` partition: every thread of the cores the exclusive and
reserved partitions name, every CPU of a named default-role partition, and any thread a partition's
`smt` leaves online (226 for the list above). Set `kubeReserved: {cpu: "226"}` or
`systemReserved: {cpu: "226"}` in the kubelet configuration. The driver checks this from the Node's
capacity and allocatable CPU: reserving more is fine (the surplus is what the node's daemons take out
of the shared default CPUs), reserving less leaves the node publishing no `dra.cpu` devices and
carrying the Node condition `CPUReservationMissing` with the value to set. On Talos, prefer
`kubeReserved`: setting `systemReserved` replaces Talos's own memory, pid and ephemeral-storage
reservations. The shared default devices publish their full CPU count whatever the daemons reserve;
the node's own accounting bounds the total.

Drain the node before you set or raise the reservation. The kubelet applies it only by restarting,
and a restarted kubelet admits every pod on the node again, oldest first, against the new, smaller
allocatable CPU: a running pod that no longer fits is rejected with `OutOfcpu`, set to `Failed`, and
its containers are stopped. `kubectl drain` leaves DaemonSet pods in place, so before the restart
check that the CPU requests of the pods still on the node, DaemonSet pods and their claims' mapped
amounts included, fit within the new allocatable CPU.

For the list above, a node short of its reservation carries `CPUReservationMissing` with:

```text
kubelet CPU reservation below 226: drain the node, then set `kubeReserved: {cpu: "226"}` or `systemReserved: {cpu: "226"}`; no dra.cpu devices are published until then
```

See
[A Node Missing Its Kubelet CPU Reservation](troubleshooting.md#a-node-missing-its-kubelet-cpu-reservation)
for the condition's other messages and what the node refuses meanwhile. Moving a `reservedCPUs` list
into a `reserved` partition raises the reservation the driver requires, since every thread of the
partition's cores counts; see [Upgrading](upgrade.md#kubelet-cpu-reservation-on-partitioned-nodes).

The driver is designed for the kubelet's default `enforceNodeAllocatable: [pods]`, which caps all
pods' memory, PIDs and hugepages at allocatable and, with the CPU reservation, lowers the CPU weight
of the `kubepods` cgroup on the shared CPUs. Keep it. The driver does not check it, since it never
reads the kubelet's configuration.

The reservation is needed because the scheduler and the kubelet admit a pod by comparing its CPU
request with the node's allocatable CPU less the requests of the pods already there. The CPUs outside
the `default` partition are part of that allocatable CPU, but no pod without a claim runs on them, so
without the reservation the node looks wider than it is to every scheduler, DaemonSet and static
pod. Kubernetes gives a driver no way to keep CPUs that no claim holds out of allocatable: the
node-allocatable mapping of KEP-5517 counts the devices that claims are allocated, and
[KEP-6352](https://github.com/kubernetes/enhancements/issues/6352), the nearest proposal, reserves
capacity for allocated devices too.

## Changing a node's partitions

Changing a node's profile or its partitions while claims are prepared on the node is unsupported, and
the driver handles it on a best-effort basis only. Make the change on a drained node:

1. Drain the node and wait until no pod holding a `dra.cpu` claim is left on it.
1. Change the configuration, or the node's `dra.cpu/profile` label.
1. Where the reservation the node needs changes, set it and restart the kubelet; see
   [The kubelet reservation](#the-kubelet-reservation).
1. Restart the driver pod on the node, if the change did not: the driver reads its configuration and
   the label only when it starts.
1. Uncordon the node.

When the driver starts on partitions that changed under prepared claims:

- A `reserved` partition may shrink, and may grow over CPUs that no prepared claim holds.

- Where a `reserved` partition has grown over CPUs that a prepared claim holds as exclusive CPUs, the
  driver keeps serving everything else and stops the running containers of that claim, through an
  NRI container update that has the container runtime write the container's `cgroup.kill`. It
  refuses to create or start them again, keeps the condition `CPUPartitionDegraded` `True` naming the
  claims, and its pod is not ready until the last such claim is unprepared. The pod's containers
  fail with, for example:

  ```text
  cannot create container "vm" in pod "5f1e0c9a-6b0e-4c1f-9a53-2d4f7e8b1c30": reserved partition "system" now holds CPUs 4-5 of prepared claim default/vm-1 (request "vcpu"); its containers are stopped: delete the claim's pod or keep those CPUs out of the partition
  ```

  Delete the pod; see
  [A CPU Partition Whose SMT the Kernel Contradicts](troubleshooting.md#a-cpu-partition-whose-smt-the-kernel-contradicts).

- A prepared claim's CPUs that lie in another partition after the change are no one's share, and no
  container without claims runs on them, until the claim is unprepared. `dra_cpu_misplaced_claims_total`
  counts such claims; see [Metrics](metrics.md).

Moving a node from upstream's mode to `- name: default` is not a drop-in change: `dra.cpu` then
gives shares where it gave exclusive CPUs. Move the claims that need CPUs of their own to an
`exclusive` partition's class first.

## Device taint rules

A DeviceTaintRule with `effect: NoExecute` evicts every pod that holds a claim on a device it
selects and does not tolerate the taint. A rule that reaches the `default` partition's shared device, by naming the device, the
node's pool or the driver, evicts every pod holding a share of it, which can be every claim pod of
the node. To stop new claims on a device, use `effect: NoSchedule`. Before a `NoExecute` rule, create
it with `effect: None`, whose status message counts the pods that `NoExecute` would evict. For the
rest, cordon and drain the node.
