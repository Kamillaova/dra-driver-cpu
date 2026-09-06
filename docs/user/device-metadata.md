# Device Metadata Files

A container holding a `dra.cpu` claim gets a read-only JSON file per request describing the devices
that request was given. It is the in-band answer to "what did I actually get?", so a workload need not
be told out of band what its own claim says.

The file is a Kubernetes feature ([KEP-5304](https://github.com/kubernetes/enhancements/issues/5304)),
written by the DRA framework rather than by this driver; what the driver decides is which attributes
go in it.

## Where it is

```text
/var/run/kubernetes.io/dra-device-attributes/resourceclaims/{claimName}/{requestName}/dra.cpu-metadata.json
```

One file per request of the claim, mounted read-only, and only into the containers that hold the
claim. The last segment carries the driver's name, so two drivers serving one request do not collide.

A claim generated from a ResourceClaimTemplate is not named in the pod spec at all — its name is
generated — so its files sit under a different subdirectory, and the segment is the `podClaimName`,
the name `pod.spec.resourceClaims[].name` gives it:

```text
/var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/{podClaimName}/{requestName}/dra.cpu-metadata.json
```

`{requestName}` is the request's own name. A request that offered the allocator alternatives keeps
that name for the directory; which alternative was chosen appears inside the file, as
`requests[0].name` in the form `<request>/<subrequest>`.

## What is in it

The file is a **JSON stream**: the same content encoded once per schema version the driver configured,
in the order that driver listed them, one object per line. A reader walks the stream and takes the
first object whose `apiVersion` it understands, the way a client negotiates versions with the API
server, so a driver lists its newest version first. This driver configures one version,
`metadata.resource.k8s.io/v1beta1`, so today the file holds a single object —
a reader that assumes that will break when a second version is added, and one that walks the stream
will not.

```json
{
  "kind": "DeviceMetadata",
  "apiVersion": "metadata.resource.k8s.io/v1beta1",
  "metadata": { "name": "vm-0-vcpus-abc12", "namespace": "compute", "uid": "…", "generation": 1 },
  "podClaimName": "vcpus",
  "requests": [
    {
      "name": "vcpus",
      "devices": [
        {
          "driver": "dra.cpu",
          "pool": "worker-1",
          "name": "cpudevcache002",
          "attributes": {
            "dra.cpu/numCPUs": { "int": 16 },
            "dra.cpu/cacheL3ID": { "int": 2 },
            "dra.cpu/threadsPerCore": { "int": 2 },
            "dra.cpu/partition": { "string": "vm" },
            "dra.cpu/role": { "string": "exclusive" },
            "resource.kubernetes.io/numaNode": { "int": 0 },
            "dra.cpu/allocatedNumCPUs": { "int": 16 },
            "dra.cpu/relocatable": { "bool": false },
            "dra.cpu/alignment": { "string": "BestEffort" },
            "dra.cpu/cpuset": { "string": "8-15,136-143" }
          }
        }
      ]
    }
  ]
}
```

Only the request the file belongs to appears in `requests`; the other requests of the same claim have
their own files.

## Which attributes this driver publishes

Every attribute the device carries in its ResourceSlice — see
[Device Attributes](device-attributes.md) — plus these, which exist only here and never in a
ResourceSlice:

| Attribute                  | Type   | Meaning                                                                                                       |
| :------------------------- | :----- | :------------------------------------------------------------------------------------------------------------ |
| `dra.cpu/allocatedNumCPUs` | int    | How many CPUs of the device's capacity this claim consumed.                                                   |
| `dra.cpu/relocatable`      | bool   | Whether the claim permits the driver to change its CPUs while the container runs.                             |
| `dra.cpu/alignment`        | string | What the claim asked about landing on more caches than it needs, defaulted when the claim said nothing.       |
| `dra.cpu/cpuset`           | string | The CPUs this request was given — **present only when they cannot change**, see below.                        |

`relocatable` and `alignment` are the claim's own
[opaque configuration](opaque-cpuset-overrides.md) read back, so a launcher can see the contract it is
running under rather than being configured with it twice.

## It is fixed at container start

The framework writes the file before the container starts and replaces it by rename. A bind mount
follows the inode, not the name, so a container that is already running keeps the file it started
with: nothing the driver learns afterwards can reach it. The framework's own hook for filling metadata
in late runs before the pod starts for the same reason.

That is why `dra.cpu/cpuset` is conditional. It appears where the CPUs are settled for the life of the
container:

- a **claimed pool**, which is never moved; and
- an **exclusive request of a claim that permits no move**, whose CPUs are then fixed for as long as it
  runs.

It is absent for the exclusive requests of a claim that states `relocatable: true`, because
[defragmentation](defragmentation.md) may move those CPUs and a file that cannot be corrected must not
name them. Such a workload reads its current CPUs from the kernel — `sched_getaffinity(2)`, or its own
`cpuset.cpus.effective` — exactly as
[Workload Configuration Requirements](workload-requirements.md) requires. Everything else in the file
is static for the claim's life and safe to read once.

## Reading it

```bash
# For the pod of hack/examples/qemu-ccx-vm/, whose podClaimName and request are both "vcpus":
# the first object whose apiVersion this reader understands, and the CPUs it names.
jq -c 'select(.apiVersion == "metadata.resource.k8s.io/v1beta1")' \
  /var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/vcpus/vcpus/dra.cpu-metadata.json |
  head -n 1 |
  jq -r '.requests[0].devices[].attributes["dra.cpu/cpuset"].string // empty'
```

`hack/examples/qemu-ccx-vm/` shows the other half in a real launcher: what to do when the answer is
absent because the claim may be moved.
