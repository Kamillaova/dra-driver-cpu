# Writing the Host's Cgroup Tree

Under `cgroupThreadedMode` `create` and `place` the driver writes the host's cgroup2 tree itself,
which [Kubelet Cgroups and QoS](kubelet-cgroups-and-qos.md) discourages for the kubelet's own
cgroups. This page says why the threaded modes need the writes, and how they stay clear of what
that page warns about. The user side is [Threaded Cgroups](../user/threaded-cgroups.md).

## Why NRI Is Not Enough

NRI adjusts a container's own cgroup: its cpuset, its CPU limit and its other resources. It cannot
create a cgroup below the container's, set a cgroup's type, enable controllers for its children, or
hand files to the container's user, and the threaded groups need all of these. So the driver
writes them directly, from its `StartContainer` handler, which NRI calls after the runtime has
created the container's process and before that process runs.

## What the Driver Writes

Below the container's thread root, the cgroup that holds the container's first process:

- each group's directory, with `threaded` in its `cgroup.type`;
- `+cpu +cpuset` in the thread root's `cgroup.subtree_control`;
- each group's `cpuset.cpus`, `cpu.max` and `cpu.weight`;
- the owner of the thread root's `cgroup.procs` and of each group's `cgroup.procs` and
  `cgroup.threads`, the user the container's process runs as;
- under `place`, the container's process ID in the `place` group's `cgroup.procs`, last.

Every write tolerates having been made already, so a repair after a restart writes the same files
again. The driver never writes the thread root's own `cpuset.cpus`, `cpu.max` or `cpu.weight`, nor
any cgroup above it: the kubelet and the runtime keep those.

## The Kubelet Page's Concerns

- **Systemd desync.** The driver writes only below the container's cgroup, which systemd does not
  manage: under the systemd cgroup driver the container's scope is delegated to the runtime. The
  end-to-end tests reload systemd on a node and find every group's CPUs and limit unchanged.
- **Path fragility.** The driver takes the container's cgroup from NRI, which reports the runtime's
  path, turns the systemd driver's `slice:prefix:name` form into a path, and finds the thread root
  by the process it holds, never by a name, so crun's sub-cgroups need no special case.
- **Blast radius.** The driver changes no limit of the pod or the container. A group's CPUs lie
  within the container's cpuset, and a group's cap applies to the threads placed in it alone.

## Privileges

The writes need the host's cgroup2 tree mounted read-write and the host's pid namespace, through
which the driver reads the user of the container's process. Under `place`, moving another
container's process into a group of other CPUs needs `CAP_SYS_NICE` where the process's permitted
capabilities are not all among the driver's, as for a root, a privileged or a capability-added
container, and moving it across cgroup namespaces needs `CAP_SYS_ADMIN` and `CAP_SYS_PTRACE` where
the host's cgroup2 mount carries `nsdelegate`. The chart grants each mode what it needs, and the driver
checks it at start; see [Configuration](../user/configuration.md#threaded-cgroups-the-drivers-privileges).
