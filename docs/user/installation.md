# Installation

The [Quickstart](quickstart.md) walks through an install with a
verification step after each stage, ending with a pod running on exclusive CPUs. This page
is the reference: compatibility, runtime setup, security, uninstall, and migration.

## Compatibility

Kubernetes requirements:

| Version       | Status                                                                            |
| ------------- | --------------------------------------------------------------------------------- |
| 1.33 or older | Not supported.                                                                    |
| 1.34–1.35     | Supported. Grouped mode (default) requires `DRAConsumableCapacity` enabled.       |
| 1.36          | Supported.                                                                        |
| 1.37          | Recommended. `DRANodeAllocatableResources` should be enabled for best experience. |

Recommended Kubernetes setup

- prefer uniform environments with no version skew between the control plane and the kubelet
- cluster (Kubernetes') feature gate enablement state for non-GA features should be reflected in
  the driver configuration. The driver cannot autodetect these, so this is a cluster admin responsibility.

Other requirements:

| Requirement           | Minimum                                                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Kernel cgroup version | **Cgroup v2**. The driver does not support cgroup v1.                                                                     |
| Container runtime     | containerd **2.0** or CRI-O **1.30** (NRI and CDI enabled by default)                                                     |
| Kubelet configuration | CPUManager disabled: `cpuManagerPolicy: none` — see [Configuration](configuration.md#kubelet-configuration-prerequisites) |

Optional features need additional cluster feature gates.
All the driver options which depends on the availability of a Kubernetes feature controlled by a Feature Gate require a double opt-in mechanism.
First, the driver must be made aware of Kubernetes feature gate status (see above). Then, driver features can be enabled or disabled.

| Feature                           | Kubernetes feature gate       |
| --------------------------------- | ----------------------------- |
| PCIe root attributes              | `DRAListTypeAttributes`       |
| NodeAllocatable resources mapping | `DRANodeAllocatableResources` |

The driver is Linux-only and needs node-level privileges (host networking, and NRI socket,
CDI directory, and kubelet plugin directory access) — see
[Security considerations](#security-considerations).

The deployment must preserve the [singleton ownership
invariant](how-it-works.md#how-it-works).

## Installing with Helm

If needed, create a kind cluster. We have one in the repo, if needed, that
can be deployed as follows:

```bash
make kind-cluster
```

The recommended way to install the driver is via the provided Helm chart:

```bash
helm install dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu -n kube-system
```

See the [Helm chart README](../../deployment/helm/dra-driver-cpu/README.md) for the full list of configuration options.

Nodes that partition their CPUs differently are configured by
[profiles](configuration.md#per-node-profiles) in one release. One release per kind of node, each
with its own `nodeSelector` or `affinity` and its own `reservedCPUs`, is still supported and
deprecated: profiles replace it. The nodes of one cluster differ only in what a profile carries;
releases that give any other setting different values are not supported.

Besides the driver DaemonSet, the chart installs the cluster-scoped `dra.cpu` `DeviceClass`
that workload claims reference — verify it with `kubectl get deviceclass`. It selects the
devices of the default partition. A `cpuPartitions` list, fleet-wide or in any profile, adds
one `dra.cpu.<name>` `DeviceClass` for each other partition that is not `reserved`, selecting
that partition's devices alone. The DeviceClass
names are fixed, so a cluster that runs several releases of the chart keeps `deviceClass.create`
enabled in only one of them. Give each release its own `nodeSelector` or `affinity`, since
the default selects every node and a node must run exactly one driver. Do not reuse
`fullnameOverride` across releases: it replaces the release name in the ClusterRole,
ClusterRoleBinding and admission policy names. Uninstall the release that owns the DeviceClass last: removing it
deletes the DeviceClass, and claims on the other releases' nodes stop allocating until it
exists again.

For environments with incomplete or synthetic sysfs topology, e.g. Docker Desktop for macOS see the [sysfs overlay example](../../hack/examples/sysfs-overlay/README.md). It demonstrates how to supply an overlay through `driverConfig.sysfsOverlay`, volume mounts, and volumes.

## Container runtime setup

The driver relies on [NRI (Node Resource Interface)](https://github.com/containerd/nri) to pin containers to their
allocated CPUs, and on [CDI (Container Device Interface)](https://github.com/cncf-tags/container-device-interface) to
inject the allocated cpuset into the container environment.

### Minimum Runtime Requirements

Both NRI and CDI are enabled by default in modern container runtimes:

| Runtime    | NRI enabled by default | CDI enabled by default |
| ---------- | ---------------------- | ---------------------- |
| containerd | 2.0+                   | 2.0+                   |
| CRI-O      | 1.30+                  | always                 |

Both runtimes also ship with the following CDI spec directories configured by default:

```toml
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
```

No manual runtime configuration is needed if you are running one of the versions above or newer.

### Manual Configuration for Older Runtimes

If you are running an older version of containerd (pre-2.0), you need to manually enable CDI and NRI in the containerd
configuration (typically `/etc/containerd/config.toml`) and restart containerd.

Enable CDI:

```toml
[plugins."io.containerd.grpc.v1.cri"]
  enable_cdi = true
  cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
```

Enable NRI:

```toml
[plugins."io.containerd.nri.v1.nri"]
  disable = false
  disable_connections = false
  plugin_config_path = "/etc/nri/conf.d"
  plugin_path = "/opt/nri/plugins"
  plugin_registration_timeout = "5s"
  plugin_request_timeout = "5s"
  socket_path = "/var/run/nri/nri.sock"
```

After editing the config, restart containerd:

```bash
systemctl restart containerd
```

### Requiring the driver's NRI plugin

While the driver is not connected to NRI, for example while its pod restarts, containerd creates
containers without it, so they are not pinned to the CPUs of their claims. containerd's NRI
default validator can refuse those containers instead, on a whole node or only for the pods that
hold `dra.cpu` claims. The driver connects to NRI as `dra.cpu`.

#### Every container on the node

Enable the default validator with the driver as a required plugin, and name an annotation, any
key you choose, that lets a pod start without it:

```toml
[plugins."io.containerd.nri.v1.nri".default_validator]
  enable = true
  required_plugins = ["dra.cpu"]
  tolerate_missing_plugins_annotation = "example.com/tolerate-missing-nri-plugins"
```

While no driver is connected, containerd then refuses every container on the node that does not
carry that annotation set to `true`, the driver's own included. Set it on the driver pod through
the chart's `podAnnotations`:

```yaml
podAnnotations:
  example.com/tolerate-missing-nri-plugins: "true"
```

Set it on the control plane's static pods too, or leave the validator off on control-plane nodes.
The containerd configuration and the static pod manifests belong to the node's provisioning, not
to the chart. Every other container on the node is refused until the driver connects.

#### Only the pods that hold claims

Enable the default validator without required plugins:

```toml
[plugins."io.containerd.nri.v1.nri".default_validator]
  enable = true
```

and annotate each pod that holds a `dra.cpu` claim:

```yaml
metadata:
  annotations:
    required-plugins.noderesource.dev: '["dra.cpu"]'
```

containerd then refuses only those pods' containers while the driver is not connected. A pod
without the annotation is not guarded. Static pods cannot hold claims, so they never wait for the
driver.

#### Limits of the check

- containerd checks a container when it creates it, never when it starts it.
- containerd counts the driver present before it calls it. So the first container created after
  the driver's connection breaks is not refused, and neither is one whose creation the driver
  does not answer within `plugin_request_timeout` (2 seconds unless set): containerd closes the
  connection and creates the container without the driver.
- With [profiles](configuration.md#per-node-profiles) and `assumeUnsolicitedUpdatesSafe` off, the
  driver closes its connection once at start, after it reads the node's profile, so the first
  container created right then is not refused either. On containerd 2.4.0 and later set
  `assumeUnsolicitedUpdatesSafe`, and the driver does not reconnect.

Without either setting, the driver refuses to prepare claims while it is not connected to NRI,
and the kubelet retries the preparation later. The kubelet prepares a pod's claims only when it
creates the pod's sandbox, so this guards a pod's first start, not a container that restarts in
a running pod.

## Security considerations

The driver needs node-level privileges: `hostNetwork: true`, and hostPath mounts for the NRI
socket (`/var/run/nri`), the CDI spec directory (`/var/run/cdi`), and the kubelet plugin
directories.

## RBAC and admission policies

With `rbac.create: true`, the default, the chart creates the driver's ClusterRole and its
binding, and one ValidatingAdmissionPolicy and binding for each kind of object the driver's
service account writes. RBAC cannot keep an account that runs on every node to one node's
objects, so the policies admit a write of that account only when it stays on the node its token
names: events whose source host is that node, that Node's status with nothing changed but the
driver's own conditions, that node's ResourceSlices, and the `dra.cpu` entries in the status of
a claim allocated on that node.

## Health probes

The driver's HTTP server listens on `--bind-address`, which the chart sets from `healthzPort`,
and serves two probe endpoints:

- `/livez` answers 200 as soon as the server serves. The chart's liveness probe uses it.
- `/healthz` answers 200 only while the driver is registered with the kubelet and connected to
  NRI, and 503 before that and while the driver reconnects to NRI. The chart's readiness probe
  uses it.

The manifests under `manifests/base`, which the chart does not render, keep the liveness probe on
`/healthz`, so a driver that is alive but not yet registered or connected to NRI is restarted
there.

## Kubernetes API client

The driver's Kubernetes API client sends at most 20 requests per second, with bursts of 50,
unless `driverConfig.qps` and `driverConfig.burst` set other limits; see
[Configuration](configuration.md#driverconfig-sub-fields).

## Upgrading the driver

Upgrade with Helm:

```bash
helm upgrade dra-driver-cpu oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu -n kube-system
```

The chart's `updateStrategy` value is unset by default, so the DaemonSet takes the Kubernetes
default, a rolling update with `maxUnavailable: 1` and `maxSurge: 0`: on each node the old driver
pod stops before the new one starts. The kubelet plugin library the driver is built on requires
`maxSurge: 0` unless the driver runs in the library's rolling-update mode, which needs a driver
that keeps no state between calls. This driver keeps the CPUs of its claims in memory and does not
use that mode.

Each node goes without a driver from the old pod's stop until the new one is running:

- The kubelet can neither prepare nor unprepare `dra.cpu` claims until the new driver registers.
  A pod that holds one does not start, and a deleted pod whose claim no other pod uses stays
  `Terminating`.
- 30 seconds after the old driver stops, the kubelet deletes the node's `dra.cpu` ResourceSlices,
  unless the new driver has connected by then. The new driver publishes them again when it starts.
- Where containerd requires the driver's NRI plugin, it refuses containers until the new driver
  connects to NRI; see [Requiring the driver's NRI plugin](#requiring-the-drivers-nri-plugin).
- The new pod is created only once the old one has stopped, so pulling its image adds to the gap.
  Pull the new image onto the nodes beforehand: with the chart's default
  `image.pullPolicy: IfNotPresent`, the kubelet then does not pull it again.

## Uninstalling

Remove the pods that hold `dra.cpu` claims while the driver still runs: the kubelet finishes
deleting the last pod that uses a claim only after the driver has unprepared the claim, and
without a driver that pod stays `Terminating`.

1. Stop whatever recreates those pods.

1. Delete them and wait until they are gone.

1. If containerd's `required_plugins` names `dra.cpu`, remove it and restart containerd, or
   containerd refuses containers once the driver is gone; see
   [Requiring the driver's NRI plugin](#requiring-the-drivers-nri-plugin).

1. If Flux manages the release, suspend its HelmRelease, or Flux installs the chart again.

1. Delete the driver's DaemonSet and wait for its pods to end, then uninstall the chart. A
   stopping driver writes the conditions it keeps on its Node `Unknown`. `helm uninstall` deletes
   the DaemonSet and the driver's ClusterRole together and does not wait for the pods, so a
   driver still stopping would no longer be allowed to:

   ```bash
   kubectl delete daemonset dra-driver-cpu -n kube-system --cascade=foreground --wait
   helm uninstall dra-driver-cpu -n kube-system
   ```

1. Remove the conditions the driver kept from every Node: `CPUAllocatableMappingDropped`, which
   it keeps where `publishNodeAllocatableResourceMapping` is on.

   ```bash
   for node in $(kubectl get nodes -o name); do
     kubectl patch "${node}" --subresource=status --type=strategic \
       -p '{"status":{"conditions":[{"type":"CPUAllocatableMappingDropped","$patch":"delete"}]}}'
   done
   ```

1. If the configuration declared profiles, remove the `dra.cpu/profile` label from every Node:

   ```bash
   kubectl label nodes --all dra.cpu/profile-
   ```

Pods that are running keep their current cpusets, but nothing manages pinning or the shared
pool anymore.

## Installation via rendered manifest (deprecated)

> **Deprecated:** Manifest-based installation is deprecated in favor of the Helm chart and will be removed in a future release.
> New users should use the Helm-based installation above.

```bash
make manifests
kubectl apply -f dist/helm-manifest.yaml
```
