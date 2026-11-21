# Workload Configuration Requirements

Currently, Kubernetes has two separate systems for requesting CPU resources: standard requests in pod/container fields (`pod.spec.resources` or `pod.spec.containers[].resources`) and DRA `ResourceClaim`s.

- The Kube-scheduler uses different plugins to account for these requests, and these plugins are mutually independent. This can lead to node CPU overcommitment because the scheduler might not have a complete picture of all allocated CPUs.

- Kubelet only considers the standard CPU requests in the PodSpec for critical node-level enforcements like [QoS class](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/) assignment and cgroup hierarchy setup, ignoring CPUs allocated via DRA claims.

[KEP-5517: DRA Node Allocatable Resources](https://github.com/kubernetes/enhancements/issues/5517) addresses the **scheduler accounting** and **cgroup enforcement** parts of this discrepancy: the scheduler counts DRA-allocated CPUs against node allocatable, and the kubelet includes them in pod- and container-level cgroup limits. Changing the **QoS class** logic is an explicit non-goal of the KEP — QoS remains based strictly on standard spec requests and limits, so pods whose only resources come from claims still classify as BestEffort (see [Configuring the right QoS class with claims](#configuring-the-right-qos-class-with-claims) below).

How to configure your workloads depends on whether KEP-5517 accounting is active:

- [Before KEP-5517](#before-kep-5517-before-137-or-alpha-fg-dranodeallocatableresources-disabled) — Kubernetes before 1.37, or 1.37+ with the `DRANodeAllocatableResources` feature gate disabled (its alpha default), or the driver running without `publishNodeAllocatableResourceMapping`.
- [With KEP-5517](#with-kep-5517-137-with-alpha-fg-dranodeallocatableresources-enabled) — Kubernetes 1.37+ with the `DRANodeAllocatableResources` feature gate enabled *and* the driver deployed with `driverConfig.publishNodeAllocatableResourceMapping: true`.

To check which mode is active, inspect the driver's ResourceSlices (`kubectl get resourceslice -o yaml`): the devices include a `nodeAllocatableResources` entry only when the mapping is enabled.

**1-to-1 Claim to Container:** in both modes, this driver enforces that a specific CPU `ResourceClaim` can only be used by *one* container within or across pods. That container uses the claim whole: it names the claim without a request, or every request the claim makes of this driver, and a container that names only some of them fails to start. See [Sharing resource claims](feature-support.md#sharing-resource-claims).

## Before KEP-5517 (before 1.37 or alpha FG `DRANodeAllocatableResources` disabled)

The scheduler and kubelet are unaware of claim CPUs, so you MUST configure your pods using one of the following methods to ensure correct behavior and resource accounting:

- **Option A (Preferred): Pod Level Resources (`pod.spec.resources`)**

  - This approach is generally preferred as it more clearly defines the pod's total CPU budget and works well for pods with a mix of containers, some needing exclusive CPUs (requested via DRA) and others using shared CPUs.
  - Set `pod.spec.resources.requests.cpu` to the *sum* of all CPUs requested across all DRA claims used by containers in this pod, PLUS any additional CPUs for containers NOT using DRA claims.
  - Do not set `pod.spec.resources.limits.cpu`: a pod-level CPU limit puts a CFS quota on the whole pod, which throttles the claim containers (see [Preventing CFS Throttling on Exclusive CPUs](#preventing-cfs-throttling-on-exclusive-cpus)).
  - Containers using DRA claims omit `cpu` from their `resources.requests` and `resources.limits`.

  A complete, runnable version of this pattern is in
  [`hack/examples/pod_with_pod_level_resources.yaml`](../../hack/examples/pod_with_pod_level_resources.yaml).

  ```yaml
  # Example: Pod Level Resources
  spec:
    resources: # Pod Level Resources
      requests:
        cpu: "16" # 10 (exclusive cpu's for claim1) + 4 (exclusive cpu's for claim2) + 2 (shared cpus for sidecar1 and sidecar2)
    containers:
      - name: main-app
        image: ...
        resources:
          # Omit CPU requests and limits
          claims:
            - name: claim1
      - name: worker
        image: ...
        resources:
         # Omit CPU requests and limits
          claims:
            - name: claim2
      - name: sidecar1
        image: ...
        # Omit CPU resources, or ensure the combined requests for sidecar1 and sidecar2 do not exceed 2.
      - name: sidecar2
        image: ...
        # Omit CPU resources, or ensure the combined requests for sidecar1 and sidecar2 do not exceed 2.
    resourceClaims:
      - name: claim1
        resourceClaimName: cpu-claim-10 # Requests 10 CPUs
      - name: claim2
        resourceClaimName: cpu-claim-4  # Requests 4 CPUs
  ```

- **Option B: Container-Level Resources (No Pod Level Resources)**

  - For each container that uses a DRA CPU claim, set `spec.containers[].resources.requests.cpu` to be *exactly equal* to the number of CPUs requested in the `ResourceClaim` referenced by that container.
  - Do not set `spec.containers[].resources.limits.cpu` on those containers, for the same reason as in Option A.

  A complete, runnable version of this pattern is in
  [`hack/examples/pod_with_resource_claim_grouped_mode.yaml`](../../hack/examples/pod_with_resource_claim_grouped_mode.yaml).

  ```yaml
  # Example: Container Level Mirroring
  spec:
    containers:
      - name: my-container
        image: ...
        resources:
          requests:
            cpu: "10" # Must match the CPU count in "claim1"
          claims:
            - name: claim1
    resourceClaims:
      - name: claim1
        resourceClaimName: cpu-claim-10 # Requests 10 CPUs
  ```

On a node with `cpuPartitions`, either option counts only the amounts booked on the `default`
partition, the amounts of the claim's shared requests on the `dra.cpu` class and of its requests
there with `cpuConfig.exclusive: true`. Never count CPUs a claim
holds on an exclusive partition, nor an amount booked on a named partition of role `default`: no
container without claims runs there, so those CPUs are not the node's to give to pods.

## With KEP-5517 (1.37+ with alpha FG `DRANodeAllocatableResources` enabled)

Requires the feature gate on the API server, scheduler, and kubelets, and the driver deployed
with `driverConfig.publishNodeAllocatableResourceMapping: true`.

The driver then publishes a `nodeAllocatableResources` mapping on every device, or, on a node that
uses `cpuPartitions`, on the devices of the `default` entry alone, and:

- the **scheduler** counts the claim's CPUs against node allocatable `cpu`. Do not also add
  them to container requests; that counts them twice.
- the **kubelet** adds the claim's CPUs to the cgroup limits when spec limits are declared:
  - **pod-level cgroup**: added to requests (`cpu.weight`) and limits (`cpu.max`);
  - **container-level cgroup**: added to limits (`cpu.max`) only, and only for containers
    that declare a cpu limit. Container `cpu.weight` stays derived from the spec requests
    only; this does not disadvantage the container on its claim CPUs, because the driver
    removes those CPUs from the [shared pool](how-it-works.md) and no other workload
    competes there.

### Recommended Pod Configuration

When deploying workloads with exclusive CPU claims, the recommended pattern is:

1. **Omit `cpu` requests and limits** on the container referencing the exclusive claim. This prevents the kubelet from setting a CFS bandwidth quota (`cpu.max`), avoiding throttling on dedicated cores.
1. **Specify `memory` requests and/or limits** matching the container's memory needs. This assigns the pod to the **Burstable** QoS class, providing better protection against eviction and out-of-memory (OOM) termination than BestEffort.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: low-latency-exclusive-workload
spec:
  containers:
  - name: exclusive-app
    image: my-app:latest
    resources:
      # Omit cpu requests and limits for the claim container
      requests:
        memory: "4Gi"   # Memory requests ensure Burstable QoS & eviction protection
      limits:
        memory: "4Gi"
      claims:
      - name: exclusive-cpus
  resourceClaims:
  - name: exclusive-cpus
    resourceClaimTemplateName: exclusive-cpus-template
```

A complete, runnable version of this pattern is available in [`hack/examples/pod_with_resource_claim_node_allocatable.yaml`](../../hack/examples/pod_with_resource_claim_node_allocatable.yaml).

### Configuring the Right QoS Class with Claims

The kubelet determines the [QoS class](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/) strictly from standard `spec` requests and limits — DRA claims do not affect it.

When running workloads with exclusive CPU claims, selecting the appropriate QoS class is essential for [preventing CFS throttling](#preventing-cfs-throttling-on-exclusive-cpus) while maintaining eviction protection.

| QoS Class                        | How to Configure                                                                         | CFS Quota (Throttling) | Eviction & OOM Safety                                                              |
| :------------------------------- | :--------------------------------------------------------------------------------------- | :--------------------- | :--------------------------------------------------------------------------------- |
| **Burstable**<br>*(Recommended)* | Set `memory` requests/limits;<br>**omit `cpu` requests and limits** on claim containers. | ✅ **No quota set**    | ✅ **Better protected than BestEffort** (OOM score scales with the memory request) |
| **Guaranteed**                   | Set `requests == limits` for both `cpu` and `memory` on every container.                 | ⚠️ **Quota set**       | ✅ **Most protected** (lowest OOM score, fixed)                                    |
| **BestEffort**                   | Omit all `cpu` and `memory` requests and limits in all containers.                       | ✅ **No quota set**    | ❌ **Evicted and OOM-killed first**                                                |

- Non-claim containers (such as logging sidecars) can specify standard CPU requests and limits normally; they run on the driver's [shared pool](how-it-works.md).
- When using **pod-level resources** (`pod.spec.resources`), set pod-level `memory` requests and omit pod-level `limits.cpu`.

#### Preventing CFS Throttling on Exclusive CPUs

A primary motivation for requesting exclusive CPUs is to achieve predictable execution by avoiding CFS quota-induced throttling and latency spikes.

When exclusive CPUs are assigned via DRA claims, declaring `limits.cpu` in `pod.spec` (even when attempting to achieve Guaranteed QoS) causes the kubelet to enforce a CFS bandwidth quota that will throttle polling threads during period boundaries. To avoid throttling, follow the **Burstable** recommendation above by omitting `cpu` requests and limits on the claim container (and at the pod level if using pod-level resources).

> For an in-depth technical explanation of Linux cgroup v2, Kubelet cgroup managers, and CFS quota mechanics, see the [Kubelet Cgroups and QoS Deep Dive](../dev/kubelet-cgroups-and-qos.md).

**Reserved environment variables:** the `DRA_CPUSET_*` environment variable prefix is reserved for the driver's CDI injection — do not set variables with this prefix; containers with malformed `DRA_CPUSET_*` values are rejected during creation. The driver injects `DRA_CPUSET_<claimUID>_<request>` for each request a container holds; its value is always `dynamic`, and no workload should read it. See [How it Works](how-it-works.md).

**Claim ownership:** on containerd 2.3.0 and later, which tells the driver the CDI devices it injects into a container, the driver refuses a container whose `DRA_CPUSET_*` variable names a claim or request whose device the container was not given, so a pod cannot take another pod's claim by setting the variable. On CRI-O and on containerd before 2.3.0 the driver cannot check this, and the first container that names a prepared claim gets it.

**Assigned CPU set:** Containers using CPU ResourceClaims receive `DRA_EXCLUSIVE_ASSIGNED_CPUSET` in Linux cpuset format (for example, `2-3,8`). It contains the union of the exclusive CPUs assigned to that container across all its CPU claims.
Containers without CPU claims do not receive it, and neither do containers whose claims hold only shared requests, whose CPUs can move. The driver sets this variable; workloads should not set or override it.
The CPUs of each request, its CPU request and its CPU limit are in the request's [device metadata file](device-metadata.md).

**Shared requests:** keep a shared request, such as one on the `dra.cpu` class of a node with `cpuPartitions`, out of a claim's `matchAttribute` constraints. A constraint ties every request it names to devices with the same value, so it would tie the claim's exclusive requests, a VM's cores for example, to the NUMA node of the shared device.

### Capped Shared Requests

A shared request is capped at its limit: the `limit` its claim writes in `cpuConfig`, or, where it
writes none, its amount, so an amount above `0` alone is the request's limit. `limit: max` removes
it. The driver writes the container's CPU quota (`cpu.max`) as that share of each 100 ms period,
`250m` as 25 ms, and a limit below `10m` as 1 ms, the kernel's minimum. A shared request without a
limit and without an amount, or with an amount of `0`, is not capped. The cap needs a kernel built
with `CONFIG_CFS_BANDWIDTH`, see [Installation](installation.md).

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata:
  name: vmm
spec:
  spec:
    devices:
      requests:
      - name: vmm-cpu
        exactly:
          deviceClassName: dra.cpu
          capacity:
            requests:
              dra.cpu/cpu: 250m # booked against the node's CPU, and the cap without a limit
      config:
      - requests: [vmm-cpu]
        opaque:
          driver: dra.cpu
          parameters:
            apiVersion: v1alpha1
            cpuConfig:
              limit: "1" # the cap; max for none
```

A limit is a quantity above `0` or `max`, at least the amount, and needs an amount, which may be `0`;
see [the opaque parameters](opaque-cpuset-overrides.md#opaque-parameters-schema-v1alpha1) for the
rules Prepare refuses a claim by.

A cgroup has one CPU quota, so a capped request takes one device and is alone in its container's
cgroup:

- Prepare refuses a capped request with `count` above 1 or with `allocationMode: All`, and a claim
  that holds a capped request beside any other request of this driver: the container that uses the
  claim holds every request of it in its one cgroup, where an uncapped share would run under the cap
  and exclusive CPUs would be throttled. The pod stays `ContainerCreating` with a
  `FailedPrepareDynamicResources` event.
- The driver refuses a container whose claims together put a capped request beside any other
  request, and the container shows `CreateContainerError`. Give each capped request a container of
  its own.

The driver writes the cap only where the container arrives without a CPU quota from the kubelet. A
CPU limit on the container, or a pod-level CPU limit it inherits, makes the kubelet set a quota of its
own, which then caps the container instead of the claim, and with KEP-5517 on the kubelet adds the
claim's mapped amount to that limit. A CPU limit can be set without anyone writing it in the pod: a
namespace `LimitRange` with a `default` limit, a VerticalPodAutoscaler, or a mutating admission
webhook can put one on every container. When a container starts with such a quota while its claim
asks for a cap, the driver posts a `CPULimitReplacesCap` Warning event on its pod and refuses
nothing. A CPU limit an in-place resize adds later replaces the cap the same way, with no event. A kubelet with `cpuCFSQuota: false` sends no quota even for a CPU limit, so there the
driver's cap stays.

Limitation: one limit per cgroup. A request with a limit on the shared default CPUs must be alone in
its container: any other request beside it in the same container is refused. When you hit this: a
workload needs more of the shared default CPUs than one device offers, for example with
`groupBy: uncorecache` and 8 shared CPUs per cache a request for 8500m fits no single cache. The
amount has to be split into two requests, one per cache, and those two parts cannot be limited
together. Why the limits are not simply added together: a limit is one CPU-time budget for the whole
cgroup; with two parts in one budget, either part could spend all of it, on any of the caches, so a
request that was meant to stay small could use the large part's share, on CPUs it was never meant to
load. Status: open for discussion upstream; DRA has no way to request one amount across several
devices.

## Claims Shared Between Pods

A `dra.cpu` claim serves one container: once a container uses it, the driver refuses every other
container that names it until the claim is unprepared, and the kubelet unprepares a claim only once
no pod on the node references it (see
[Sharing resource claims](feature-support.md#sharing-resource-claims)). Give each pod its own
claim, for example from a `ResourceClaimTemplate`. A claim that more than one pod uses keeps the
later pods waiting:

- **A standalone claim that two pods reference.** Unless the claim has a node-allocatable mapping,
  the scheduler reserves it for both pods. The second pod's container is refused for as long as
  that pod exists, even after the first pod is gone, since the second pod still references the
  claim.
- **A force-deleted pod.** A pod force-deleted while it holds a standalone claim leaves the API
  before the kubelet has unprepared the claim, so the claim can be allocated to a new pod. If the
  new pod lands on the same node before the kubelet starts to unprepare the claim, the kubelet
  gives it the old preparation without asking the driver, and if the old pod's container had been
  created, the new pod's container is refused for as long as the new pod exists. Do not
  force-delete a pod that holds a standalone `dra.cpu` claim. If one was, delete the new pod of
  that claim as well, and create it again once the old pod's teardown has finished.
- **A pod group.** A claim reserved for a pod group, with the `DRAWorkloadResourceClaims` feature
  gate, serves one container of one pod too: the driver refuses the containers of the group's
  other pods while that binding lasts.

## Preemption

The scheduler's preemption frees no `dra.cpu` devices: a pending pod whose claim finds no free
`dra.cpu` device does not preempt lower-priority pods that hold one. Free capacity by deleting or
moving claim pods.

## Extended Resource Claim Status integrations

Kubernetes `status.extendedResourceClaimStatus` is for DRA-backed extended resources. [Extended resource names](https://kubernetes.io/docs/tasks/configure-pod-container/extended-resource/) exclude standard resources such as `cpu` and `memory`, so `extendedResourceName` in a `DeviceClass` or a pod's `status.extendedResourceClaimStatus` is not expected to work with this CPU DRA driver when the container only requests native `cpu`.

For example, a Pod that references a CPU `ResourceClaim` explicitly through `containers[].resources.claims` follows this driver's supported path. A Pod that only patches `status.extendedResourceClaimStatus` with `requestMappings[].resourceName: cpu` does not, because `cpu` is a native resource rather than a DRA-backed extended resource.

For integrations that model native `cpu`, use the Kubernetes node-allocatable DRA status path when available instead.

## Device Health Reporting

The driver reports the health of every device it manages to the kubelet over the DRA `WatchHealthStatus` gRPC API, so the kubelet can reflect it in `pod.status.containerStatuses[].allocatedResourcesStatus`.

- `Healthy`: the default state. The driver has not observed any failure for this device.
- `Unhealthy`: reserved for failures attributable to the device itself. The driver does not currently report this for any condition. A claim that fails to prepare (for example, a CDI spec write error) surfaces as a claim error instead, not a device health change.
- `Unknown`: reported by the kubelet itself and not the driver, when it stops receiving health updates for a device within its lease window (for example, if the driver process is down).
