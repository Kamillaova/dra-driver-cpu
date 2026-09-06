# CPU Defragmentation

> [!IMPORTANT]
> This is a feature of [this fork](../../FORK.md), not of upstream `dra-driver-cpu`.

As claims come and go, the CPUs backing the surviving ones scatter. A claim that started inside one
uncore cache (an AMD CCX, or one slice of a split L3) ends up straddling two, and the node's free CPUs
end up one per cache, so no cache can be handed to a future claim whole. Neither recovers on its own:
placement is chosen once, when the claim is prepared.

Defragmentation moves a **running** claim onto different CPUs to recover that alignment, without
restarting its container. The claim keeps its CPU count and its NUMA node; only which CPUs back it
change.

```yaml
# values.yaml
driverConfig:
  cpuDeviceMode: grouped
  groupBy: numanode
  assumeUnsolicitedUpdatesSafe: true # see Runtime support
  defragEnabled: true
```

## What it does not do

- **It does not change a claim's size.** A claim allocated 4 CPUs always has 4.
- **It does not move a claim between NUMA nodes.** The driver never sets `cpuset.mems`, so a claim's
  memory locality is exactly its CPUs' NUMA footprint, and a move preserves it.
- **It is invisible to the scheduler.** A move changes neither a device's `capacity` nor any claim's
  `consumedCapacity`, so no `ResourceSlice` is republished, no scheduler cache is invalidated, and no
  write reaches the API server.
- **It cannot fix a bad node choice.** The scheduler sees only how many CPUs are free on a node, never
  their shape, so it can bind a large claim to a node that genuinely cannot free a cache while a
  neighbour could. A bound claim cannot move to another node.
- **It is a no-op on nodes with one uncore cache per NUMA node**, where there is no spread to recover.
  A NUMA node whose CPUs report no uncore cache ID cannot be reasoned about at all; `/placements`
  lists it under `unmeasurableNUMANodes` rather than failing.

## Requirements

`defragEnabled` is refused at startup unless all of the following hold.

- **`cpuDeviceMode: grouped` with `groupBy: numanode` or `socket`.** These are the modes where the
  driver chooses a claim's CPUs in the first place. In `individual` mode the scheduler picks exact
  per-CPU devices, and with `groupBy: machine` the cpuset comes from the claim's own opaque config, so
  in both cases the placement is not the driver's to change.
- **`assumeUnsolicitedUpdatesSafe: true`.** See below.

### Runtime support

A move is a container update the runtime did not ask for. On a runtime whose vendored NRI predates
[containerd/nri#301](https://github.com/containerd/nri/pull/301), such an update can deadlock the
runtime: it takes an internal lock before dispatching to the plugin and needs the same one to serve
the update.

The driver cannot detect this. The NRI `Configure` handshake reports the runtime's name and version
but not its vendored NRI version, and a version table would mis-classify backported builds in both
directions. So it is an operator assertion:

- **containerd** carries the fix from NRI v0.12.1 onwards, first released in containerd
  v2.4.0-beta.0. Earlier releases do not: containerd v2.3.4, which `kindest/node:v1.37.0` ships,
  still vendors NRI v0.12.0, so a stock kind cluster does **not** meet this floor. Verify a runtime
  rather than trusting its version string: `go version -m $(which containerd) | grep containerd/nri`.
- **CRI-O v1.36** still vendors NRI v0.12.0 and is **not** supported. It also never populates
  `Container.CDIDevices`, which this driver uses to verify that a claim a container names really was
  injected into it.

## What a workload must not do

A container's environment cannot be rewritten once it exists, so the injected `DRA_CPUSET_*` variable
cannot track a claim that moves. With `defragEnabled` its value is the literal string `dynamic` rather
than a cpuset, so a workload that parses it fails loudly instead of quietly pinning itself to CPUs its
claim has left.

Read the current CPUs from the kernel instead:

- `sched_getaffinity(2)`, which the kernel keeps current through a move; or
- the container's own `/sys/fs/cgroup/cpuset.cpus.effective`.

A workload that self-pins is re-homed by the kernel with no signal of its own. To be told when its
CPUs change, watch its own `cpuset.cpus` with inotify: that file is written by `runc`/`crun` on every
update and so generates `IN_MODIFY`, unlike the derived `cpuset.cpus.effective`, which is produced on
read. Read `.effective` (or call `sched_getaffinity`) once the event arrives, since that is the value
that survived intersection with the parent cgroup. Two changes inotify cannot see are CPU hotplug and
a parent cgroup's cpuset changing; a lazy backstop poll covers both.

## When a pass runs

There is nothing to tune. A pass runs when something changed: after every prepare, after every
release, and after the driver synchronizes with the runtime, which is what repairs a node the driver
was not running on. A round that committed anything asks for the next pass itself, because the CPUs
it freed are what the following move needs, so a repair that needs several rounds runs them back to
back rather than waiting between them.

A pass plans and applies one round per NUMA node, which is what bounds how much of a machine one
batch disturbs; a claim is never moved between NUMA nodes anyway. A node whose round the runtime
refused or never confirmed is tried again on its own, after a delay that grows while it keeps
failing and resets when it succeeds, and it holds up no other node meanwhile. A quiet node runs no
passes at all.

A pass is one read of the online CPU set and then pure computation over the claims the node already
holds, and it stops as soon as it finds a NUMA node as well packed as its claims allow, so an arrival
that lands aligned — the normal case on a node with free caches — costs about a millisecond and moves
nothing.

**Who pays.** A pass repacks largest claim first, so a large misplaced claim is repaired by moving the
small claims standing in its way. That is what best-effort placement means for the smaller claims, and
it belongs in their expectations. With no misplaced claim, nothing moves at all: small claims keep
their private caches while there is slack.

## Observing it

The metrics are in [Metrics](metrics.md#defragmentation);
`dra_cpu_defrag_largest_alignable_free_cpus` is the one that says whether the *next* large claim will
land aligned.

When a claim stays split and the metrics do not say why, ask the node directly. `/placements` serves
the current placement of every claim, and `?dryrun=1` adds the moves a pass would make right now
along with the gate that stopped it. A dry run reserves nothing, writes nothing and sends nothing.

```bash
# On the node
curl -s localhost:8080/placements?dryrun=1

# Or centrally, through the API server's pod proxy (needs pods/proxy)
kubectl get --raw \
  "/api/v1/namespaces/dra-driver-cpu/pods/$(kubectl -n dra-driver-cpu get pod \
     -l app.kubernetes.io/name=dra-driver-cpu \
     --field-selector spec.nodeName=<node> -o jsonpath='{.items[0].metadata.name}'):8080/proxy/placements?dryrun=1"
```

A `plan.reason` of `all N moves towards the ideal are blocked` means the claims in the way are
themselves pinned, or there is no slack to move them through. A `movingFrom` is a move still in
flight: the claim holds both sets of CPUs until the runtime confirms it.

## Rolling out

Passes are node-local and write nothing to the API, so 100 nodes are 100 independent instances with no
coordination between them and no cluster-scale load. The flip side is that a planner bug reaches every
node of a given topology at once: **stage a rollout by node type, not at random.**

Rolling *back* to a driver without this feature needs the node drained first. See
[Migration](../../FORK.md#migration-between-upstream-and-this-fork).
