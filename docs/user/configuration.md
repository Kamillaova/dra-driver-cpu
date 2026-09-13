# Configuration

This page covers configuring the driver itself, the kubelet prerequisites required to run it,
and the command-line flags currently being deprecated in favour of the configuration file.

## Driver configuration

The driver supports two configuration mechanisms:

- The config file - the main, preferred way to configure the driver.
- Command-line flags — kept for a small set of important, mostly behavior altering settings and for backward compatibility. Some flags are being deprecated in favor of their config file equivalent (see [Helm: deprecated args values vs driverConfig](#helm-deprecated-args-values-vs-driverconfig) below).

If the same setting is provided both ways, the explicit command-line flag wins, so avoid mixing
the two for the same field.

### Configuration file

The config file is a YAML file passed to the driver via `--config <path>`.

When deploying with Helm, you don't write this file yourself: set the `driverConfig` value in
your values file and the chart serializes it to YAML, stores it in a ConfigMap, mounts it as
`/etc/dracpu/config.yaml` inside the driver container and passes `--config` automatically. For
example, with a `values.yaml` containing:

```yaml
# values.yaml
# Driver configuration
driverConfig:
  cpuDeviceMode: individual
  reservedCPUs: "0-1"
# Other tuning knobs of Helm chart
image:
  tag: v0.2.0
resources:
  requests:
    cpu: "200m"
    memory: "100Mi"
nodeSelector:
  kubernetes.io/os: linux
```

```console
helm install dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu -f values.yaml
```

Individual fields can also be tuned with `--set` instead of a values file, e.g. to switch
`cpuDeviceMode` to `individual` and reserve CPUs `0-1`:

```shell
helm install dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu \
    --set driverConfig.cpuDeviceMode=individual \
    --set driverConfig.reservedCPUs="0-1"
```

#### driverConfig sub-fields

The config file is a flat YAML map - there are no nested groups. All fields are optional
except where noted. Unknown fields are rejected at startup to catch typos early.

These fields only affect the driver's own behavior (CPU allocation, hostname, sysfs path,
etc.). Anything about the driver's Pod itself - image, resources, node placement, and so
on - is configured through other Helm values, not through this file.

`apiVersion` (string)

- When present, must be `v1alpha1`. Rejected otherwise.

`cpuDeviceMode` (string, default: `grouped`)

- `individual`: exposes each allocatable CPU as a separate device in the `ResourceSlice`.
  This mode provides fine-grained control, as it exposes granular information specific
  to each CPU as device attributes.
- `grouped`: exposes a single device representing a group of CPUs. This mode treats CPUs
  as a [consumable capacity](https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/5075-dra-consumable-capacity/README.md)
  within the group, improving scalability by reducing the number of API objects.

`groupBy` (string, default: `numanode`)

- Grouping strategy used when `cpuDeviceMode` is `grouped`.
- `numanode`: groups CPUs by NUMA node.
- `socket`: groups CPUs by socket.
- `machine`: groups all allocatable node CPUs into a single machine-wide capacity device.
  NOTE: this mode requires an external scheduler to supply core assignments. See
  [Custom Opaque CPUSet Allocation Overrides](opaque-cpuset-overrides.md).

`reservedCPUs` (string)

- CPUs excluded from allocation and from the `ResourceSlice`, given as a cpuset, e.g.
  `"0-1"`. This has the same semantics as the kubelet's `static` CPU Manager policy with
  [`strict-cpu-reservation`](https://kubernetes.io/blog/2024/12/16/cpumanager-strict-cpu-reservation/)
  enabled and [`reservedSystemCPUs`](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#explicitly-reserved-cpu-list)
  set. For correct CPU accounting, the number of CPUs reserved here should match the sum
  of the kubelet's `kubeReserved` and `systemReserved` settings, so that the kubelet
  subtracts the right number of CPUs from `Node.Status.Allocatable`.

`hostnameOverride` (string)

- Override the node hostname the driver registers under.

`kubeconfig` (string)

- Path to a kubeconfig file (for out-of-cluster use).

`publishNodeAllocatableResourceMapping` (bool, default: `false`)

- Publish `nodeAllocatableResources` mappings in ResourceSlice devices, so the scheduler
  and kubelet account claimed CPUs as node allocatable `cpu`.
- Requires the `DRANodeAllocatableResources` feature gate (alpha, 1.37+). See
  [Workload Configuration Requirements](workload-requirements.md).

`fullPhysicalCPUsOnly` (bool, default: `false`)

- Allocate whole physical cores, so a core's SMT siblings are never split between two
  claims, nor between a claim and the shared pool. This is the driver equivalent of the
  kubelet CPU Manager's
  [`full-pcpus-only`](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy-options)
  policy option, and it matters where CPU siblings must not be shared between tenants.
- Requires `cpuDeviceMode: grouped`: in `individual` mode the scheduler picks exact
  per-CPU devices, so the driver cannot keep a core's siblings together.
- A no-op where SMT is disabled, since every core has one thread.

`assumeUnsolicitedUpdatesSafe` (bool, default: `false`)

- Permit the driver to push container cpuset updates the runtime did not ask for. Every
  feature that reacts to a change without waiting for a container lifecycle event needs
  this, including `reconcileSharedOnUnprepare`.
- It is an operator assertion, not something the driver can detect. Unsolicited updates
  deadlock runtimes whose vendored NRI predates
  [containerd/nri#301](https://github.com/containerd/nri/pull/301), and the NRI `Configure`
  handshake reports the runtime version but not its NRI version, so there is no reliable
  check. containerd carries the fix from NRI v0.12.1 onwards, first released in containerd
  v2.4.0-beta.0; containerd v2.3.4 and CRI-O v1.36 do not.

`reconcileSharedOnUnprepare` (bool, default: `true`)

- Widen shared containers onto the CPUs a claim released as soon as it is unprepared,
  rather than leaving them on the narrower cpuset until their next `CreateContainer` or
  driver restart.
- Requires `assumeUnsolicitedUpdatesSafe` and is inert without it, so enabling that one
  option is enough.

`defragEnabled` (bool, default: `false`)

- Let the driver move a running claim onto different CPUs to recover uncore cache (CCX/L3)
  alignment lost to claim churn, without restarting its container. A claim keeps its CPU
  count and its NUMA footprint; only which CPUs back it change.
- Requires `cpuDeviceMode: grouped` with `groupBy: numanode` or `socket`, because those
  are the modes where the driver chooses a claim's CPUs in the first place: `individual`
  mode has the scheduler pick exact per-CPU devices, and `groupBy: machine` takes the
  cpuset from the claim's own opaque config.
- Requires `assumeUnsolicitedUpdatesSafe`, since a move is pushed to the runtime
  unprompted.
- A structural no-op on nodes with one cache per NUMA node, where there is no spread to
  recover. See [CPU Defragmentation](defragmentation.md).
- Workloads must not read their CPUs from the `DRA_CPUSET_*` environment variable, whose
  value is fixed when the container starts. With this option on, the variable's value is
  the literal string `dynamic` instead of a cpuset, so a workload that parses it fails
  loudly rather than pinning itself to CPUs its claim has left. Read
  `sched_getaffinity(2)` or the container's own `cpuset.cpus.effective` instead. See
  [How it Works](how-it-works.md).

`cachePlacementStrategy` (string, `pack` | `spread`, default: `pack`)

- How a claim that fits inside one uncore cache chooses among the caches that can hold it. `pack`,
  the default and upstream's behaviour, fills the fullest cache that fits: clean caches stay whole
  for larger claims, small tenants share L3. `spread` fills the emptiest cache: each small claim gets
  a cache of its own while there is slack — L3 isolation first — and a whole-cache claim arriving
  later relies on defragmentation to consolidate the small tenants out of its way, rather than on
  caches having been hoarded against its possible arrival. Claims no cache can hold take the largest
  caches first under either policy.
- Both allocation and defragmentation draw placements from the same policy-aware selector, so the two
  can never disagree about where a claim belongs. `spread` works with or without
  `fullPhysicalCPUsOnly`; without it, a claim still avoids splitting a physical core where it can,
  but two claims may end up on one core's SMT siblings once the chosen cache offers nothing better —
  whole-core allocation is what actually forbids that, and isolation-minded deployments should run
  both. Requires `cpuDeviceMode: grouped`: in individual mode the scheduler names exact CPU devices
  and the driver picks nothing.

`cpuPartitions` (list of partition, default: empty)

- Describe the node's cores as named partitions, each with a `name`, a `role`, an explicit `cpus`
  cpuset of whole cores, and an optional `smt` expectation. The CPUs no partition names form the
  implicit partition `default`, which is the one a claim reaches without naming a partition, so the
  name `default` may not be declared.
- `role` says what the cores are for. `reserved`: no container ever runs there, the successor of
  `reservedCPUs`. `default`: devices are published and containers without a claim run on whatever is
  left unclaimed. `shared`: a pool a workload reaches by claiming it. `exclusive`: devices are
  published and no container without a claim ever runs there.
- `smt` is how many threads per core the partition expects the platform to leave online: `true` (the
  default) accepts whatever the hardware has, `false` means one thread per core, and an integer is an
  upper bound. The driver verifies the expectation against the running kernel and never changes
  hotplug state itself, so offlining siblings stays the machine configuration's job.
- Requires `cpuDeviceMode: grouped`. `reservedCPUs` in the same scope is an error rather than a merge:
  each describes the same CPUs from the other end, and a `reserved` partition is how the list says it.
- Names are DNS labels of at most 46 characters, since the device names built from them must stay
  DNS labels. Partitions may not overlap.
- The chart renders one `DeviceClass` per named partition, `dra.cpu-<name>`, and the `dra.cpu` class
  selects the implicit partition alone. Classes are cluster-scoped while partitions are per node
  type, so the chart covers the partitions of every profile and a name two node types share yields
  one class. A claim reaches a named partition by naming its class and tolerating
  `dra.cpu/partition=<name>` with `operator: Equal`; an existence toleration with an empty key would
  match every taint and is not what a template should carry.

```yaml
driverConfig:
  cpuDeviceMode: grouped
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

`profiles` (map of string to profile, default: empty)

- One `cpuPartitions` list per node type, and nothing else in a profile. CPU numbering is a property
  of the node's hardware, so a fleet mixing node types cannot state one list for all of them —
  `"1,17"` is a whole core on one part and two half cores on another. Every other field stays
  fleet-wide policy.
- A node selects its profile with the **`dra.cpu/profile` node label**; the driver reads it at
  startup. Changing the label takes effect on the next driver restart, deliberately: the partitions
  are the ground truth under every current placement, not a value to swap live. Naming a profile the
  config does not declare fails that node's driver loudly — a typo must not run a node on another
  node type's cores.
- Declaring any profile makes `reservedCPUs` and `cpuPartitions` outside a profile errors, and a node
  without the label refuses to start rather than picking up a description meant for a different part.
  A node whose cores are all interchangeable is labelled `dra.cpu/profile=default`: that profile
  always exists, is never declared, and leaves the whole node in the implicit `default` partition.
- Every profile is validated on every node at startup, so a broken profile fails the fleet at rollout
  rather than one node type at its next reboot.
- With no profiles at all, the fleet-wide `reservedCPUs` and `cpuPartitions` still apply, and the
  driver logs that they are on their way out.

```yaml
driverConfig:
  cpuDeviceMode: grouped
  profiles:
    r7625:
      cpuPartitions:
        - name: system
          role: reserved
          cpus: "0,128"
        - name: dataplane
          role: exclusive
          cpus: "8-15"
          smt: false
        - name: vm
          role: exclusive
          cpus: "16-127,144-255"
    x3d:
      cpuPartitions:
        - name: system
          role: reserved
          cpus: "0,16"
        - name: vm
          role: exclusive
          cpus: "1-15,17-31"
```

```console
kubectl label node worker-a dra.cpu/profile=x3d
kubectl label node --selector=node.kubernetes.io/instance-type=epyc-bare dra.cpu/profile=r7625
kubectl label node worker-z dra.cpu/profile=default
```

#### Example

```yaml
# values.yaml
driverConfig:
  apiVersion: v1alpha1
  cpuDeviceMode: grouped
  groupBy: numanode
  reservedCPUs: "0-3"
```

#### Versioning and backward compatibility

The schema is versioned via the optional `apiVersion` field (currently `v1alpha1`). The layout
is intentionally flat for now. If a nested hierarchy is introduced in the future, the
`apiVersion` field will be bumped so that older config files continue to be accepted or produce
an error.

### Helm: deprecated args values vs driverConfig

`driverConfig` is the Helm value that generates the config file described above (a single map
covering all driver settings — there is no separate Helm value per field). `args.*` on the other
hand exposes individual fields as explicit Helm values.

- `args.cpuDeviceMode`, `args.groupBy`, `args.reservedCPUs`, and `args.hostnameOverride` are
  deprecated: instead of being emitted as CLI flags, they are now folded into the generated
  `driverConfig` ConfigMap.
- Both reach the same driver settings; `args.*` takes priority when both are set for the same
  field.
- The intent is to eventually deprecate `args.*` entirely in favour of `driverConfig` as the
  single configuration mechanism. The driver logs the effective configuration at startup so you
  can verify which values are active.

### Helm values validation and compatibility

The chart rejects unknown top-level values, so a misspelled value name fails validation before
anything is rendered rather than being ignored. Helm's reserved `global` table stays accepted,
and values whose keys you pick, such as `podLabels` and `resources.limits`, are untouched.

If a chart value is removed in a later release, an upgrade that carries the previous release's
values forward can fail validation. Helm carries them when no new values are supplied at all,
with `--reuse-values`, and with `--reset-then-reuse-values`. Validation runs before anything is
rendered or applied, so the running release is untouched: drop the stale key and upgrade again,
or pass `--reset-values` with the values you want in full.

Values that only your own tooling reads belong outside the file you hand to Helm. They were
never chart values, so they are rejected rather than ignored.

### Command-line flags

**NOTE:** Command-line flags are kept mainly for backward compatibility. Prefer the
[configuration file](#configuration-file) above for new deployments.

`--cpu-device-mode`, `--group-by`, `--reserved-cpus`, `--hostname-override`, `--sysfs-overlay`
are deprecated in favour of their config file equivalents and will be removed in a future
release ([issue #245](https://github.com/kubernetes-sigs/dra-driver-cpu/issues/245)). Each is
marked as deprecated in `--help`, and passing one explicitly logs a startup warning.

- `--cpu-device-mode` → `cpuDeviceMode`
- `--group-by` → `groupBy`
- `--reserved-cpus` → `reservedCPUs`
- `--hostname-override` → `hostnameOverride`
- `--sysfs-overlay` → `sysfsOverlay`

The remaining flags aren't part of that deprecation:

- `--config`: path to the config file described above.
- `--kubeconfig`: path to a kubeconfig file, for out-of-cluster use. Also settable via the `kubeconfig` config field.
- `--bind-address`: address the metrics server listens on.
- `--expose-pcie-roots`: adds the `resource.kubernetes.io/pcieRoot` standard value to CPU
  devices, reporting the PCIe roots close to each device. Since it always reports values
  as a list, this option requires the cluster feature gate `DRAListTypeAttributes` (see
  KEP 5491) to be enabled. The driver cannot introspect the cluster feature gate, so
  enable the feature gate first and this option second. Unlike the flags above, it is not
  deprecated and has no config file equivalent — it is intentionally excluded from the
  config file (see [driverConfig sub-fields](#driverconfig-sub-fields) above) and stays a
  standalone flag (or Helm `args.exposePCIeRoots`).
- `--kubelet-root-dir`: the kubelet's own root directory (default `/var/lib/kubelet`).
  Running the binary directly, or writing the manifests by hand, the flag is the way to set
  the root, together with matching `<root>/plugins` and `<root>/plugins_registry` mounts.
  Like `--expose-pcie-roots`, it is not deprecated and has no config file equivalent: the
  chart owns this value, not `driverConfig`.

## Kubelet configuration prerequisites

**IMPORTANT:** The kubelet's CPUManager implements assignment of exclusive CPUs to workloads. The CPUManager and this DRA driver are mutually incompatible and only
one can be enabled at a time on any given node. You need to disable the CPUManager on the nodes you wish to run this DRA driver.

1. The default settings of the kubelet are compatible with this DRA driver. If you never fine-tuned the kubelet, you are probably fine.
1. Make sure `cpuManagerPolicy: "none"` is set in the kubelet [configuration file](https://kubernetes.io/docs/tasks/administer-cluster/kubelet-config-file/).
1. If you changed the kubelet configuration, restart the kubelet to take effect. **NOTE:** you may need to [delete the CPUManager state file](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#changing-the-cpu-manager-policy).
1. You may now proceed with deploying and configuring this DRA driver.

### Using a custom kubelet root directory

If the kubelet runs with `--root-dir` set to something other than `/var/lib/kubelet`, set
`kubeletRootDir` to that root's absolute path (relative paths are refused, since a hostPath
can't follow the kubelet's working directory). The path is cleaned, not resolved: `..` and
repeated separators are collapsed, and symlinks are left to the kubelet:

```yaml
# values.yaml
kubeletRootDir: /mnt/data/kubelet
```

That one value becomes the driver's `--kubelet-root-dir` and both hostPath mounts.

Leaving `kubeletRootDir` out, or setting it to YAML `null`, selects `/var/lib/kubelet`. Helm
drops a null key while coalescing values, so by the time the chart is rendered it looks the
same as a release installed before this value existed, and both have to mean the standard
root. An explicit empty string is refused instead.

The flag is newer than the chart's `appVersion`, which still selects the v0.2.0 image, so a
source-checkout install with a relocated root also needs an image built from a revision that
knows the flag — otherwise every node exits on an unknown flag:

```bash
helm install dra-driver-cpu ./deployment/helm/dra-driver-cpu -n kube-system \
  --set-string kubeletRootDir=/mnt/data/kubelet \
  --set-string image.repository=REGISTRY/REPOSITORY \
  --set-string image.tag=IMAGE_TAG
```

A released chart's `appVersion` already knows the flag, and a default install (root left
unset) emits no flag at all — neither needs this.

A single release's DaemonSet lands on every node it selects, so those nodes must agree on
the kubelet root; splitting them across releases fails with `invalid ownership metadata`,
since the chart's `DeviceClass` is cluster-scoped under a fixed name. A cluster whose nodes
disagree needs the driver deployed some other way until the chart can vary the root per
node.
