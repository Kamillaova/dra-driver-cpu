# Threaded Cgroups

A container has one cgroup, so all its threads share one cpuset and one CPU limit. A VM's vCPU
threads want the CPUs its claim holds exclusively, with no limit, while its housekeeping threads
want a share of the node's `default` partition under the share's cap. A claim can split the cgroup
of the container that uses it into threaded groups, one cgroup each, with its own CPUs, CPU limit
and weight, and the workload then places each of its threads in the group it belongs to. cgroup
v2's threaded mode lets the threads of one process sit in different cgroups, and `cpu` and
`cpuset` are threaded controllers.

The driver builds the groups when the container starts, under the cgroup that holds the
container's first process: the container's own cgroup, or under crun a cgroup below it. That cgroup
is the group's thread root. The node must run [`cgroupThreadedMode`](configuration.md#driverconfig-sub-fields)
`create` or `place` on cgroup v2, which the threaded modes require; the driver does not support
cgroup v1 in any mode (see [Installation](installation.md#compatibility)).

## The Claim's Block

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata:
  name: vm
spec:
  spec:
    devices:
      requests:
      - name: vcpu
        exactly:
          deviceClassName: dra.cpu.vms
          capacity:
            requests:
              dra.cpu/cpu: "2"
      - name: vmm-cpu
        exactly:
          deviceClassName: dra.cpu
          capacity:
            requests:
              dra.cpu/cpu: 250m
      config:
      - requests: [vmm-cpu]
        opaque:
          driver: dra.cpu
          parameters:
            apiVersion: v1alpha1
            cpuConfig:
              limit: 500m
      - opaque:
          driver: dra.cpu
          parameters:
            apiVersion: v1alpha1
            cgroupConfig:
              threaded:
                enabled: true
                place: vmm-cpu
                groups:
                - name: vcpu
                  requests: [vcpu]
                - name: vmm-cpu
                  requests: [vmm-cpu]
```

- `enabled` switches the split on. Left out or `false`, the block changes nothing, and the driver
  accepts it without a warning.
- `groups` is the complete list of groups. Left out, each request with a result of this driver
  gets a group named after it. `groups: []`, and a group with `requests: []`, are refused.
- `place` names the group the container's process starts in, on a node running `place`.

The block applies to the whole claim: write it in a config entry without `requests`, or in one that
lists every request. Two entries that carry `cgroupConfig` are refused, equal or not.

## Rules

- With `groups`, every result of this driver lands in exactly one group. A group's `requests` name
  requests or alternatives of a `firstAvailable` request (`helper/shared`), and a result goes to the
  group that names it or its request. A request in no group is refused. A group may hold several
  requests, for example the vCPUs of a VM on two NUMA nodes.
- Group names are DNS labels, unique in the list.
- `place` names a group that receives a result: one that names a request whole, or every
  alternative of one. On a node running `create`, a claim with `enabled: true` that names `place` is
  refused. A claim without `place` works in both modes.
- A capped shared request is alone in its group, and a group that holds exclusive CPUs takes no
  cap. An exclusive request on a `default` partition counts as exclusive CPUs.
- Alternatives of one request may sit in different groups. A group that receives no result on an
  allocation is not built.
- A name in `requests` that matches no result of this driver on the allocation is ignored, so
  another driver's request may be named. A request and one of its own alternatives named together
  are refused. A group named like another group's request is allowed.
- `place` may name a group that holds only exclusive CPUs. The process then starts there with no
  cap until its threads move.
- Two threaded claims may share a container when their groups have different names and at most
  one of them names `place`. The container's cgroup then holds both claims' CPUs. A threaded
  container refuses a claim of this driver that is not threaded, whose requests would run in the
  thread root.

[Troubleshooting](troubleshooting.md#a-threaded-claim-that-is-refused) gives each refusal.

## The Modes

- `create`: the driver builds the groups, writes each group's `cpuset.cpus`, `cpu.max` and
  `cpu.weight`, and hands the thread root's `cgroup.procs` and each group's `cgroup.procs` and
  `cgroup.threads` to the user the container's process runs as. It moves no process: the workload
  first moves itself into its group, with one write of its process ID to `<group>/cgroup.procs`,
  then places its threads.
- `place`: as `create`, and the driver then moves the container's process into the `place` group
  before the process runs, so the workload only places its threads.

[Configuration](configuration.md#driverconfig-sub-fields) gives the values `cgroupThreadedMode` takes,
how to write them in YAML, and the privileges each mode needs. Every device publishes the node's
mode as `dra.cpu/cgroupThreadedMode`, so a threaded claim's selector can keep it off nodes that would
refuse it:

```yaml
selectors:
- cel:
    expression: device.attributes["dra.cpu"].cgroupThreadedMode != "off"
```

## Where a Workload Finds Its Groups

Each entry the driver writes in the claim's `status.devices` names the group the result's CPUs land
in, beside its partition, and each request's [metadata file](device-metadata.md) carries the same
name as `dra.cpu/group`. A claim without `enabled: true` gets neither. A group's name never changes
for the claim's life.

```yaml
status:
  devices:
  - driver: dra.cpu
    pool: node-a
    device: cpudevnuma000-vms
    data:
      apiVersion: v1alpha1
      kind: ClaimPlacementStatus
      partition: vms
      cpuset: "16,144"
      group: vcpu
```

The container finds its groups under `/var/run/dracpu/cgroup`, a link to the thread root as the
container sees it, in a read-only directory the driver mounts in every container that holds a
threaded claim. A workload opens `/var/run/dracpu/cgroup/<group>`, whatever the runtime, the cgroup
driver, crun's sub-cgroup or its `run.oci.delegate-cgroup` annotation. vm-operator's daemon, for
example, reads the group's name from the claim status and passes cloud-hypervisor
`/var/run/dracpu/cgroup/<group>`. `/proc/self/cgroup` cannot locate the thread root under `place`,
whose process starts inside a group.

A container that joins another container's cgroup namespace gets no link and is not supported; the
driver logs it. A container the runtime reports as unknown loses its link; see
[Troubleshooting](troubleshooting.md#a-threaded-container-that-does-not-start).

An agent outside the container that needs the container's cgroup in the host's terms gets nothing
from the driver. It finds it from the pod's `containerID` and `/proc/<pid>/cgroup` on the node,
less the group the process sits in under `place`. The driver could publish the path once it knows
it, at container creation, at three costs: an API write outside the preparation of the claim, a
race with readers that read the status once, and a new RBAC permission to patch claims or pods.

## Moving Threads

A thread moves into a group by writing its thread ID to `<group>/cgroup.threads`, which needs write
access to that file and to the thread root's `cgroup.procs`, both handed to the container's user.
The driver reads that user from `/proc/<pid>/status` once, when the container starts, as the host
sees it, so it is right in a pod with a user namespace too. Start the process as the user that moves
the threads: a process that later switches to another user cannot write the files.

The groups' directories, `cgroup.subtree_control`, `cpuset.cpus`, `cpu.max` and `cpu.weight` stay
root's, so the workload cannot change its CPUs or its cap. This holds for runc on the cgroupfs
driver, and for crun when the workload runs as a user other than root. It does not hold:

- under the systemd cgroup driver, where runc hands the thread root's directory, its
  `cgroup.subtree_control` and its `cgroup.threads` to the process's user when the container's
  `/sys/fs/cgroup` is writable, so the workload can move a thread into the thread root, out of its
  group's cap, and create cgroups there;
- under crun with a user namespace, and under sysbox-runc, where the container's root owns the
  thread root's `cgroup.subtree_control`;
- for a workload that runs as root without a user namespace, which owns every file the driver
  creates.

The workload can always move its whole process back into the thread root.

## Writable Cgroups

A threaded container needs a writable `/sys/fs/cgroup`. containerd mounts it read-only unless the
runtime handler sets `cgroup_writable = true` (containerd 2.1 and later); select the handler with
a RuntimeClass:

```toml
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.cgroup-writable]
  runtime_type = "io.containerd.runc.v2"
  cgroup_writable = true
```

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: cgroup-writable
handler: cgroup-writable
```

Copy the default handler's other settings, `SystemdCgroup` among them, into the new one. CRI-O
mounts it writable for a pod annotated `cgroup2-mount-hierarchy-rw.crio.io`. The driver does not
check the mount: without it the workload's moves fail with `EROFS`, and a thread that cannot move
cannot pin itself to its group's CPUs.

## Weights

The kubelet's `cpu.weight` of the container's cgroup stays as it is, and the groups split it among
themselves:

- a group that holds only exclusive CPUs weighs 1;
- a group that holds shares weighs in proportion to the CPU shares the kubelet gives a pod that
  requests their amounts, an amount of 0 counting as the kubelet's minimum of 2 shares, scaled so
  that the largest such group of the container weighs 10000, and each at least 1.

A VM with a 250m `vmm-cpu` share gets `vmm-cpu` 10000 and `vcpu` 1. Two shared groups of 250m and
500m get 5000 and 10000. What the weights cannot do:

- Busy vCPUs dilute the housekeeping: each busy vCPU adds load beside the housekeeping in the
  container's weight, so the housekeeping's part of the pod's weight falls as more vCPUs are busy.
- A busy sidecar with a CPU request takes most of the pod's weight.
- A pod whose weight is the kubelet's minimum, for example a BestEffort pod, splits that minimum.
- A task left in the thread root, such as an exec'd shell, runs at the default weight of 1024
  against the exclusive group's 1, so on that group's CPUs it outweighs the vCPU there.

## Placement Is Best Effort

Under `place`, the driver moves a process only while the runtime waits for it. A container created
or started while the driver is slower than the runtime's timeout for a plugin, or not connected,
starts unplaced, in its thread root, on all its claim's CPUs and without their caps, until it is
restarted. The driver never places a running process: moving a running VM's process into a group
would drop every vCPU pin. It announces the window at start; see
[Troubleshooting](troubleshooting.md#a-threaded-container-that-does-not-start).

In either mode, a container created while the driver is not connected gets no groups: the driver
did not see it created, so it neither judged it nor builds anything for it, and the container runs
in its thread root, on all its claims' CPUs and without their caps.

To narrow the window:

1. Set containerd's `plugin_request_timeout` a little above the kubelet's `runtimeRequestTimeout`
   (2 minutes by default), never unbounded, so a slow driver is waited for instead of skipped
   after 2 seconds. The value applies to every NRI plugin on the node, and while the driver hangs,
   every container create, start and stop on the node waits.
1. Require the driver's NRI plugin; see
   [Requiring the driver's NRI plugin](installation.md#requiring-the-drivers-nri-plugin).
1. Enable NRI's `FailurePolicy` once the runtime supports it, so that a call that times out fails
   the container instead of skipping the driver.

A container started while the driver is being upgraded on its node, with no driver running there,
starts unplaced the same way; see [Upgrading the driver](installation.md#upgrading-the-driver).

## Exec and Probes

`kubectl exec` and exec probes run in the thread root, on all the claim's CPUs, the exclusive ones
included, and with no cap:

- under runc, and under crun without annotations, in the container's cgroup;
- under crun's systemd sub-cgroup, in that sub-cgroup;
- under `run.oci.delegate-cgroup`, which the container's cgroup refuses, in the cgroup the
  container's process runs in at that moment: the delegate path under `create`, and the `place`
  group, with its cap, once the process has moved.

runc and crun can run an exec'd process in a cgroup of its own, but nothing above them asks for
one: an OCI process has no cgroup field, containerd passes none, a CRI exec request carries none,
and NRI has no exec event. An exec'd command that must not run in the thread root moves itself into
a group.

## Runtimes and Cgroup Drivers

- runc is supported under both cgroup drivers.
- crun is supported under both cgroup drivers, its systemd sub-cgroup (`container` unless
  `run.oci.systemd.subgroup` names another) and `run.oci.delegate-cgroup` included: the thread root
  is the cgroup that holds the container's process, wherever it is below the container's cgroup.
- youki is not supported: deleting a container fails while the driver's groups exist.
- sysbox-runc is not supported and not tested. It moves the container's process into `init.scope`
  below the container's cgroup, which is then the thread root, and adds a user namespace the runtime
  does not report.
- A container that shares the host's cgroup namespace, as a privileged one does, gets a link to the
  thread root's path in the host's tree.
- Nodes with SELinux enforcing are not tested.

The end-to-end tests run runc under the systemd driver, and crun under the systemd and the cgroupfs
drivers.

## Kernel

The threaded modes need a kernel built with `CONFIG_FAIR_GROUP_SCHED`, without which the groups have
no `cpu.weight`. Run a kernel built without `CONFIG_RT_GROUP_SCHED`, or with it switched off by
`CONFIG_RT_GROUP_SCHED_DEFAULT_DISABLED` or `rt_group_sched=0` on the kernel command line. Where it
is on, the kernel gives every cgroup below the root a realtime budget of zero, which cgroup v2 has
no file to raise, so no thread of any pod can run `SCHED_FIFO` or `SCHED_RR`, a workload's realtime
polling threads included, with or without this driver.
