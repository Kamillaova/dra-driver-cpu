# Configuration

This page covers configuring the driver itself, the kubelet prerequisites required to run it,
and the command-line flags the driver still supports.

## Driver configuration

The driver supports two configuration mechanisms:

- The config file - the main, preferred way to configure the driver.
- Command-line flags — kept for a small set of important, mostly behavior altering settings and for
  backward compatibility. Flags removed in v0.4.0 and their config file equivalents are listed
  in [Command-line flags](#command-line-flags) below.
  Some flags are being deprecated in favor of their config file equivalent
  (see [Helm: deprecated args values vs driverConfig](#helm-deprecated-args-values-vs-driverconfig) below).

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
  cpuDeviceMode: grouped
  reservedCPUs: "0-1"
# Other tuning knobs of Helm chart
image:
  tag: v0.3.0
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

Individual fields can also be tuned with `--set` instead of a values file, e.g. to reserve
CPUs `0-1`:

```shell
helm install dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu \
    --set driverConfig.reservedCPUs="0-1"
```

#### driverConfig sub-fields

The config file is a flat YAML map - there are no nested groups, only the `profiles` that
describe kinds of node. All fields are optional
except where noted. Unknown fields are rejected at startup to catch typos early.

These fields only affect the driver's own behavior (CPU allocation, hostname, sysfs path,
etc.). Anything about the driver's Pod itself - image, resources, node placement, and so
on - is configured through other Helm values, not through this file.

`apiVersion` (string)

- When present, must be `v1alpha1`. Rejected otherwise.

`cpuDeviceMode` (string, default: `grouped`)

- `individual`: **deprecated**. It exposes each allocatable CPU as a separate device in the
  `ResourceSlice`. For scheduler-controlled selection of exact CPUs, follow
  [Migrating from individual mode](opaque-cpuset-overrides.md#migrate-from-individual-mode).
- `grouped`: exposes a single device representing a group of CPUs. This mode treats CPUs
  as a [consumable capacity](https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/5075-dra-consumable-capacity/README.md)
  within the group, improving scalability by reducing the number of API objects.

`groupBy` (string, default: `numanode`)

- Grouping strategy used when `cpuDeviceMode` is `grouped`.
- `numanode`: groups CPUs by NUMA node.
- `socket`: groups CPUs by socket.
- `uncorecache`: groups CPUs by the uncore (L3) cache they share, one device per cache, published
  in order of NUMA node and then of the cache id the kernel reports. A request then takes its CPUs
  from one cache, and the scheduler picks which. The driver does not start on a node where the
  kernel reports no cache for an allocatable CPU, or where an allocatable CPU's cache spans NUMA
  nodes: its error names the CPUs and the reason.
- `machine`: groups all allocatable node CPUs into a single machine-wide capacity device.
  Use it only when that allocator must select from the entire node.

> [!NOTE]
> All grouping modes support integration with an external scheduler (see `allocator`) to
> supply CPU core assignments. The `machine` grouping however _requires_ the external
> allocator, while the other grouping modes work with all the allocator modes.
> See [Custom Opaque CPUSet Allocation Overrides](opaque-cpuset-overrides.md).

`allocator` (string, default: `cpumanager`)

- `cpumanager`: selects CPUs with the driver's built-in topology-aware allocator.
- `external`: works with every grouped `groupBy` value, requires an opaque cpuset from an
  external scheduler, and publishes the attributes that scheduler needs to choose CPUs. See
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

`sysfsOverlay` (string)

- Path, inside the driver container, to a YAML file that overlays the sysfs topology the
  driver discovers. Use it for incomplete or synthetic topology, e.g. Docker Desktop for
  macOS. The file must be mounted into the Pod; see the
  [sysfs overlay example](../../hack/examples/sysfs-overlay/README.md) for the file format
  and mounts.

`publishNodeAllocatableResourceMapping` (bool, default: `false`)

- Publish `nodeAllocatableResources` mappings in ResourceSlice devices, so the scheduler
  and kubelet account claimed CPUs as node allocatable `cpu`.
- Requires the `DRANodeAllocatableResources` feature gate (alpha, 1.37+). See
  [Workload Configuration Requirements](workload-requirements.md).
- The driver keeps the condition `CPUAllocatableMappingDropped` on its Node, `True` when the API
  server drops the mapping; see
  [troubleshooting](troubleshooting.md#a-node-allocatable-mapping-the-api-server-dropped).

`qps` (number, default: `20`)

- Requests per second the driver's Kubernetes API client may send. Must be greater than 0.
- The driver sends every API request through one client, the kubelet plugin library's claim
  reads and ResourceSlice writes included, so `qps` and `burst` limit all of them together.

`burst` (integer, default: `50`)

- Requests the driver's Kubernetes API client may send at once before `qps` applies. Must be at
  least 1.

`fullPhysicalCPUsOnly` (bool, default: `false`)

- Give claims whole physical cores only, the counterpart of the kubelet CPU Manager's
  `full-pcpus-only` [policy option](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#cpu-policy-static--options):
  the logical CPUs of one core never go to two claims. See
  [Whole physical cores](feature-support.md#whole-physical-cores).
- Requires `cpuDeviceMode: grouped`, and `reservedCPUs` must hold whole cores: the driver refuses
  to start otherwise. It also refuses to start on a node with a core of more than two logical
  CPUs, unless that core is wholly in `reservedCPUs`.

`assumeUnsolicitedUpdatesSafe` (bool, default: `false`)

- Widen the containers without claims onto the CPUs an unprepared claim freed as soon as it is
  unprepared, through a container update the runtime did not ask for. Without it they get those
  CPUs at the next container creation of a claim or the next NRI reconnect.
- Set it only where every node's runtime vendors NRI v0.12.1 or later, which fixes
  [containerd/nri#301](https://github.com/containerd/nri/pull/301): an older one can deadlock on
  such an update. containerd 2.4.0 vendors it; containerd 2.3 does not. The driver does not check.

`profiles` (map of profile name to profile, default: none)

- How each kind of node partitions its CPUs, by name. A node runs the profile its
  `dra.cpu/profile` label names. See [Per-node profiles](#per-node-profiles).

#### Settings published as device attributes

Every device the driver publishes carries the settings that decide whether the driver prepares a
claim, so that a claim's selector can keep it off the nodes that would refuse it after the
scheduler placed it there. Each attribute is named as its setting and is always present.

| Attribute                      | Type   | Value                      | What the driver refuses to prepare                                                               |
| ------------------------------ | ------ | -------------------------- | ------------------------------------------------------------------------------------------------ |
| `dra.cpu/fullPhysicalCPUsOnly` | bool   | `fullPhysicalCPUsOnly`     | When `true`, an amount that whole cores of the device do not make up                             |
| `dra.cpu/allocator`            | string | `cpumanager` or `external` | In grouped mode, a `cpuConfig.cpuset` under `cpumanager`, a request without one under `external` |

See [Whole physical cores](feature-support.md#whole-physical-cores) and
[Custom Opaque CPUSet Allocation Overrides](opaque-cpuset-overrides.md) for these refusals.

For example, a claim whose request names a `cpuConfig.cpuset` selects the nodes that honor it:

```yaml
        selectors:
        - cel:
            expression: device.attributes["dra.cpu"].allocator == "external"
```

and a claim for an odd number of CPUs selects the nodes that do not ask for whole cores, or whose
cores have one logical CPU:

```yaml
        selectors:
        - cel:
            expression: >-
              !device.attributes["dra.cpu"].fullPhysicalCPUsOnly ||
              device.attributes["dra.cpu"].threadsPerCore == 1
```

A selector that reads an attribute the device does not carry is a CEL runtime error, which fails
the claim's allocation instead of skipping the device. While some nodes still run a driver release
that does not publish these attributes, guard the expression with `in`:

```yaml
            expression: >-
              !("fullPhysicalCPUsOnly" in device.attributes["dra.cpu"]) ||
              !device.attributes["dra.cpu"].fullPhysicalCPUsOnly ||
              device.attributes["dra.cpu"].threadsPerCore == 1
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

See the [backward compatibility policy](upgrade.md#backward-compatibility-policy)
for the project-wide promise.

The schema is versioned via the optional `apiVersion` field (currently `v1alpha1`). The layout
is intentionally flat for now. If a nested hierarchy is introduced in the future, the
`apiVersion` field will be bumped so that older config files continue to be accepted or produce
an error.

### Per-node profiles

Profiles configure nodes that differ. CPU numbering belongs to the hardware: `"1,17"` is one whole
core on one part and two half cores on another, so a fleet that mixes kinds of node has no single
`cpuPartitions` list that is right on every node. A profile carries that list for one kind of node,
by name, and nothing else: every other field of `driverConfig` applies to every node, and the nodes
of one cluster differ only in what a profile carries.

```yaml
# values.yaml
driverConfig:
  apiVersion: v1alpha1
  cpuDeviceMode: grouped
  profiles:
    r7625:
      cpuPartitions:
        - name: default
        - name: system
          role: reserved
          cpuset: "0,128"
    x3d:
      cpuPartitions:
        - name: default
```

```console
kubectl label node --selector=node.kubernetes.io/instance-type=r7625 dra.cpu/profile=r7625
kubectl label node worker-a dra.cpu/profile=x3d
```

- A node selects its profile by the value of its `dra.cpu/profile` label, so a profile name is a
  label value: at most 63 characters, letters, digits, `-`, `_` and `.`, starting and ending with a
  letter or a digit. The driver refuses to start on a configuration declaring a profile whose name
  is not one, or whose name is empty.
- Once the configuration declares profiles, every node carries the label. On a node without it, or
  with a label naming no declared profile, the driver refuses to start rather than run the node on
  another kind of node's description. The label `default` selects only a declared profile named
  `default`. A node whose cores are all alike names a declared profile holding only
  `- name: default`, as `x3d` above does.
- Every profile's `cpuPartitions` holds the entry `- name: default`. A profile whose list lacks it,
  or is left out, `null` or empty, fails the Helm render, and the driver refuses to start on it.
- Every profile is checked on every node, so a broken profile stops the rollout rather than one
  kind of node at its next restart.
- Beside `profiles`, the fleet-wide `reservedCPUs` and `cpuPartitions` are refused, so that a node's
  CPUs are never described two ways, and so are `allocator: external` and `groupBy: machine`.
- The driver reads the label when it starts. A changed label takes effect at the driver's next
  restart.

Until it has read the label, the driver serves the container runtime without a configuration and
does not register with the kubelet. In that window only the containers without claims of
host-network pods start, such as kube-proxy and the CNI's agents, on the CPUs the runtime gives
them; the driver refuses every other container, and the kubelet creates it again later. The window
lasts as long as the driver cannot read its Node, an API server outage at start included: the
driver logs each failed read, and its readiness probe fails meanwhile.

Selecting a node's configuration from named configurations by a node label is what NVIDIA's device
plugin does with `nvidia.com/device-plugin.config`, and NVIDIA's GPU Operator for MIG layouts with
`nvidia.com/mig.config`. Those fall back to a default on a node without the label, skip a value
naming no configuration and apply a change at once; profiles refuse the first two and apply a change
at the driver's restart.

Without profiles the label configures nothing, and a node carrying it is refused: the driver reads
its Node at every start, after it connects to the container runtime and before it registers with
the kubelet, and stops on a `dra.cpu/profile` label before it publishes anything. Remove the label,
or declare the profiles the labels name. To move a fleet to profiles, change the configuration
first, then label each node and restart its driver: the driver reads the configuration and the
label only when it starts. See
[A Profile Label Without Profiles](troubleshooting.md#a-profile-label-without-profiles).

Running without profiles, the node's CPUs named, if at all, by the fleet-wide `reservedCPUs` and
`cpuPartitions`, keeps working and is deprecated: profiles replace it, each holding
`- name: default`, with every node labelled. So is running one Helm release per kind of node; see
[Installing with Helm](installation.md#installing-with-helm).

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

`--cpu-device-mode`, `--group-by`, `--reserved-cpus`, `--hostname-override` and
`--sysfs-overlay` were removed in v0.4.0
([issue #281](https://github.com/kubernetes-sigs/dra-driver-cpu/issues/281)); the driver
now rejects them as unknown flags. Set the config file equivalents instead:

- `--cpu-device-mode` → `cpuDeviceMode`
- `--group-by` → `groupBy`
- `--reserved-cpus` → `reservedCPUs`
- `--hostname-override` → `hostnameOverride` (when unset, the driver uses the `NODE_NAME`
  environment variable, which the Helm chart sets from the downward API)
- `--sysfs-overlay` → `sysfsOverlay`

The remaining flags are still supported:

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
Both mounts use hostPath type `Directory`, so if `<root>/plugins` or `<root>/plugins_registry`
doesn't exist, the pod fails to start instead of registering where the kubelet doesn't look.

After path cleaning, the value may not contain `$(` or `$$`. The kubelet can
reinterpret those in a container's argument, rewriting `$$` to a single `$` and
substituting `$(VAR)` where that variable is defined, while `hostPath.path` and
`volumeMount.mountPath` are taken as written. A root still carrying one after cleaning
can therefore reach the driver as one directory and the volumes as another.

Only those two sequences are refused, not the character. A lone `$`, as in
`/var/lib/$kubelet`, is left alone by the kubelet and names the same directory
everywhere, so it is accepted.

Cleaning comes first because that is the value the chart renders, so a sequence in a
segment the cleaning removes is not refused: `/var/lib/$(IGNORED)/../kubelet` cleans to
`/var/lib/kubelet` and is accepted. The chart does refuse both sequences outright rather
than only the ones the kubelet would act on: an unresolvable `$(VAR)` is left alone, and
the chart does not predict which variables a Pod will have.

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
the kubelet root. A cluster whose nodes disagree can run one release per node class: give
each release its own `nodeSelector` and never the same `fullnameOverride` (it replaces the
release name in the ClusterRole, ClusterRoleBinding and admission policy names), and keep the cluster-scoped
`dra.cpu` `DeviceClass`, whose name is fixed, in exactly one of them by setting
`deviceClass.create=false` in the others. Installing a second release with the DeviceClass
still enabled fails with `invalid ownership metadata`. Uninstall the release that owns the
DeviceClass last, since removing it deletes the DeviceClass that the other releases' claims
reference.
