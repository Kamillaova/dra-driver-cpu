# Upgrading

This document detail the upgrade actions that are specific to releases of
the CPU DRA driver. It complements the general [installation](installation.md)
and [configuration](configuration.md) documentation.

Upgrades must preserve the [singleton ownership
invariant](how-it-works.md#how-it-works).
An upgrade may temporarily leave a node without an active driver, but it must
not create overlapping active owners.

## Backward compatibility policy

The backward compatibility promise must be considered

- best-effort (we try hard not to break, but may happen) for all alpha-level APIs (v1alpha1, v1alpha2...)
- tentative (we will break only in exceptional circumstances) for all beta-level APIs (v1beta1, v1beta2...)
- effective (no backward incompatible changes) for all the GA APIs (v1, v2...)

The driver exposes configuration APIs in `api/`. The project strives to support
these APIs adhering to the kubernetes best practices and standards, including
backward compatibility, under the maturity graduation process described above.
The backward compatibility promise covers the Go API types and the API
YAML/JSON serialization format and semantics, subject to the import path exception below.

The backward compatibility promise does not cover command line flags.
While the project strives to cause minimal disruptions to the users, it reserves
the option to remove deprecated options after only a single release. In other words,
flags deprecated in 0.3.0 may be removed already in 0.4.0.
In some non-default cases, flags may have a longer promised lifetime; in that case,
the versions on which they will be removed will be clearly documented.

The backward compatibility promise does not cover the output of the `dracpu introspect`
command unless noted otherwise. When introspecting the driver data, the output format
may change any time even between patch releases. The output is meant for live consumption,
not for stable storage.

The golang package import paths may change as part of API package reorganizations the
project may do across releases. While such changes do break golang source compatibility
and require consumers to update imports, they do not, by themselves, change the
serialized API version or its compatibility guarantees.
The package relocations will be documented with migration instructions.
The package relocations will never be done in patch releases.
Example:

```
# before
opaqueapi "github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
# after
allocationapi "github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
```

The backward compatibility promise does not cover golang packages outside the api packages.
The project strives, but can't guarantee, backward compatible changes in the golang
packages. Package relocations, likewise golang api packages, can always happen except
in patch releases.
Public packages (not these in internal/...) must be considered to have best-effort
backward compatibility promise until documented otherwise.

## Upgrade from 0.3.0 to 0.4.0

### Action required

#### Removed command-line flags

`--cpu-device-mode`, `--group-by`, `--reserved-cpus`, `--hostname-override` and
`--sysfs-overlay` were removed. The driver now exits on startup when any of them is passed,
so manifests that set them directly will crashloop after the image upgrade.
Helm users are not affected as long as the chart and the image are upgraded together.

Move the values to the configuration file before upgrading:

- `--cpu-device-mode` → `cpuDeviceMode`
- `--group-by` → `groupBy`
- `--reserved-cpus` → `reservedCPUs`
- `--hostname-override` → `hostnameOverride`
- `--sysfs-overlay` → `sysfsOverlay`

See [configuration](configuration.md#command-line-flags) for details.

#### API packages reorganization

Go consumers of the opaque allocation configuration API must update their imports:

- Schema types: `api/v1alpha1` moves to `api/allocation/v1alpha1`.
- `ParseOpaqueConfig`: `api` moves to `api/allocation`.

The serialized `v1alpha1` schema and parsing behavior are unchanged.

#### Per-request CDI variables

A container that uses a claim now receives `DRA_CPUSET_<claimUID>_<request>=dynamic`, one
variable for each request it holds, where it received `DRA_CPUSET_<claimUID>=<cpuset>` before.
The value is always `dynamic`, never a cpuset. A workload that read its CPUs from
`DRA_CPUSET_<claimUID>` reads `DRA_EXCLUSIVE_ASSIGNED_CPUSET` instead. Claims prepared before
the upgrade keep their variable until they are unprepared.

#### Containers that use part of a claim

A container that names some but not all of a claim's requests in `resources.claims` is refused
at creation, where it ran with all of the claim's CPUs before. Name the claim without a request,
or give the container its own claim. Claims prepared before the upgrade keep the old behavior
until they are unprepared.

#### Events

The driver posts Kubernetes events, so its ClusterRole now grants `create` and `patch` on
`events`. With `rbac.create: true` the chart also creates a ValidatingAdmissionPolicy and its
binding, named after the release, which admit an event from the driver's service account only
when the event's source host is the driver's own node: the account that runs the upgrade needs
the right to create both. With `rbac.create: false`, grant the driver's service account `create`
and `patch` on `events` yourself; without them the API server refuses the driver's events, and
the driver logs each refusal.

#### Node status and admission policies

The driver can now keep conditions on its own Node, so its ClusterRole grants `patch` on
`nodes/status`. With `rbac.create: true` the chart also creates three more
ValidatingAdmissionPolicies and their bindings, named after the release, which admit a write of
the driver's service account only on the driver's own node:

- its Node's status, where nothing may change but the driver's own conditions: no other
  condition, no other status field, and none of the Node's labels, annotations or other metadata;
- its node's ResourceSlices;
- the status of a claim allocated on its node, where only the `dra.cpu` entries of
  `status.devices` may change.

The account that runs the upgrade needs the right to create them. With `rbac.create: false` the
chart creates no policy.

#### A `dra.cpu/profile` label without profiles

On a node carrying a `dra.cpu/profile` label, a driver whose configuration declares no
[profiles](configuration.md#per-node-profiles) now stops before it registers with the kubelet. Remove
the label, `kubectl label nodes --all dra.cpu/profile-`, or declare the profiles the labels name.
See [troubleshooting](troubleshooting.md#a-profile-label-without-profiles).

### Behavior changes

#### Start order

The driver now connects to the container runtime through NRI, waits for its first synchronization
and reads its own Node before it registers with the kubelet; it registered first before. The
ClusterRole already grants `get` on `nodes`. Until its first synchronization the driver is not
registered, so the kubelet prepares no claim on the node meanwhile; an API server that does not
answer at start holds the registration back as long, the driver logging each failed read.

#### Node condition for the node-allocatable mapping

With `publishNodeAllocatableResourceMapping: true`, the driver now keeps the condition
`CPUAllocatableMappingDropped` in its Node's status, through the `nodes/status` permission above:
`True` once the API server has dropped the mapping from the node's ResourceSlices, `False`
otherwise, and `Unknown` after the driver stops. With the option off, the default, the driver
writes nothing to its Node. See
[troubleshooting](troubleshooting.md#a-node-allocatable-mapping-the-api-server-dropped).

#### Kubernetes API client limits

The driver's Kubernetes API client now sends up to 20 requests per second, with bursts of 50,
where it used client-go's defaults of 5 and 10 before. Set `driverConfig.qps` and
`driverConfig.burst` to change the limits; see
[configuration](configuration.md#driverconfig-sub-fields).

#### Watchdog

The driver now watches itself, always, with no setting to turn it off. When its state lock has
been held, or an NRI hook or a Prepare or Unprepare call has been running, for more than 60
seconds, `/healthz` answers 503; past 180 seconds the driver stops and exits non-zero for the
kubelet to restart it.
See [troubleshooting](troubleshooting.md#a-stuck-driver).

### Deprecations

#### Device metadata: `dra.cpu/allocatedNumCPUs`

The `dra.cpu/allocatedNumCPUs` attribute of a request's device metadata is deprecated. Read
`dra.cpu/cpuRequest` instead: the same amount as a quantity string, such as `2`, written beside it.
The driver still writes `dra.cpu/allocatedNumCPUs`. Its removal will be announced in this document
one release before it happens.

#### Running without profiles

A configuration that declares no `profiles`, the node's CPUs named, if at all, by the fleet-wide
`reservedCPUs` and `cpuPartitions`, keeps working and is deprecated, and so is one Helm release per
kind of node. [Per-node profiles](configuration.md#per-node-profiles) replace both: declare a
profile for each kind of node, each holding `- name: default`, and label every node with its own;
a node whose cores are all alike names a profile holding only that entry. Its removal will be
announced in this document one release before it happens.

### Early warning

Before the driver enters beta stage, a batch of **breaking** changes are planned.
The idea is to coalesce together all the known backward incompatible changes in release 0.4.0.
We advice users wishing to prepare early to track the linked issues.

Anticipated breaking or possibly breaking changes are:

- breaking: memory support (https://github.com/kubernetes-sigs/dra-driver-cpu/issues/36)
  reason: requires repo and artifact rename; requires configuration file changes.
- breaking: driver identity change (https://github.com/kubernetes-sigs/dra-driver-cpu/issues/327)
  reason: identity change require existing object removal.
- possibly breaking: configuration format change (https://github.com/kubernetes-sigs/dra-driver-cpu/issues/302)
  reason: configuration overhaul. Some combination of options may need fixing.
- possibly breaking: driver persistence (https://github.com/kubernetes-sigs/dra-driver-cpu/issues/258)
  reason: implementation dependent.

## Upgrade from 0.2.0 to 0.3.0

### Action required

#### Helm

Upgrade the existing Helm release in place. Do not uninstall the release
first: uninstalling removes the driver DaemonSet and stops CPU allocation and
shared-pool reconciliation on every node.

Example command:

```bash
helm upgrade dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu -n kube-system
```

#### Configuration: grouped mode

If you use grouped mode with `groupBy: machine`, set the allocator explicitly
to `external` as part of the upgrade:

```yaml
driverConfig:
  cpuDeviceMode: grouped
  groupBy: machine
  allocator: external
```

This requirement was implicit prior to 0.3.0 and it is now explicit.
Failing to set the `external` allocator will cause the driver to fail to startup.

If you use grouped mode with `groupBy: machine`, make sure all the claims use
exact or fistAvailable device request count to 1. Different request counts will
cause the driver to fail at claim preparation stage.
This constraint was previously implicit prior to 0.3.0 and it is now explicit.

#### Configuration: individual mode

Individual mode is deprecated. For scheduler-controlled selection of exact CPUs, follow
[Migrating from individual mode](opaque-cpuset-overrides.md#migrate-from-individual-mode).

### Action recommended

#### Previous installations using `install.yaml`

`install.yaml` was the manifest used to install the driver in the `0.1.0` release and is now
obsolete. It has since been replaced by the rendered manifest above and, preferably, the Helm
chart. If you still have a cluster running the `install.yaml`-based installation, use the steps
below to migrate to the Helm chart.

Because the DaemonSet label selectors differ between `install.yaml` (`app: dracpu`) and the Helm chart
(`app.kubernetes.io/name`, `app.kubernetes.io/instance`), and DaemonSet selectors are immutable, an
in-place migration is not possible. The only practical migration path is a delete and reinstall:

```bash
# Step 1: remove the legacy manifest-managed resources
# (use the same manifest file that was originally applied)
kubectl delete -f <legacy-manifest>.yaml

# Step 2: install the Helm-managed release
helm install dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu -n kube-system
```

**Disruption:** Deleting the DaemonSet terminates the driver pods on all nodes simultaneously. During
the migration window, no new CPU allocations can be made and the shared-pool cpuset updates stop.
Existing workloads are not evicted and their CPUs should remain. Once the new DaemonSet is scheduled
and the driver pods are running, the driver should recover its state.
