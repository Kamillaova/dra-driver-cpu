# E2E test suite

The E2E tests want to manage the CPU allocation on the tested nodes.
The CPU allocation state is ultimately shared node state. Thus, the test should run serially
because it's simpler to ensure correctness and reliable runs (avoid flakes).

## extending the E2E test suite

While considering extending the testsuite, adding tests, cases or sub-suites, please consider the following:

- The E2E tests are the closest, often identical, representation of user flows. The E2E test do have a very important
  signal and value, but they also are the costliest of the tests. To run them, we need to setup a full cluster.
  In general, prefer adding unit or integration tests, because they are cheaper, so they can run faster and more often.
- Related to the previous point, is very unlikely that a "right number" of E2E tests even exists.
  The project need to evaluate on a case-by-case manner, and the "right number" of E2E test may change over time.
- The single most important characteristic of a E2E test is its signal. Good E2E test should
  1. represent a real user flow and
  1. do it so reliably.
     Flakes should be avoided and actively mitigated. The experience strongly suggests that is better to have a small
     set of representative, high value, highly reliably tests than a larger set of flakier tests.

### adding or enhancing e2e tests

When changing the e2e suite codebase, make sure to use a `Fixture` object the e2e utility packages provide.

```golang
import (
    //...
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
    //...
)

var _ = ginkgo.Describe("Your suite", ..., func() {
	var (
		rootFxt *fixture.Fixture
	)

	ginkgo.BeforeEach(func(ctx context.Context) {
        //...
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create root fixture: %v", err)
        //...
    })

//...
```

The fixture object will transparently provide:

- a `logr.Logger` you can use for debug information; prefer `ginkgo.By` to document test steps
- a `kubernetes.Interface` client enhanced to address the testsuite needs (e.g. object labelling)
  you can use to manipulate kubernetes objects.

## configure using environment variables

The following environment variable allow to change the behavior of the e2e tests.
However, some Makefile targets also behave differently depending on the value of some variables;
this applies to Makefile targets which are meant to simplify or implement testing/CI flows.
In general, it is safe to assume that makefile targets beginning with `ci-` or `test-e2e-`
honor these settings, where applicable.

- `DRACPU_E2E_VERBOSE`: (optional) if set (any nonempty value; if unsure, use `1`),
  *the Makefile* will emit extra debug messages while executing targets.
  Please use the go logging configuration for the e2e tests proper.
  There's no support for verbosity levels.

- `DRACPU_E2E_LABEL_FILTER`: (optional) a [Ginkgo label-filter expression](https://onsi.github.io/ginkgo/#spec-labels)
  used by the Makefile `test-e2e` target to select which specs to run. For example,
  set it to `metrics` to run only metrics specs, or `!negative` to exclude specs
  labeled `negative`.

- `DRACPU_E2E_TEST_IMAGE`: (mandatory) the full pullSpec of the test image the suite should
  use as container image to run test containers. The default CI configuration sets this
  value automatically.

- `DRACPU_E2E_TARGET_NODE`: (optional) the CPU-related tests don't need any specific node.
  If this variable is set, it should be the `hostname` of a valid worker node in
  the cluster against which the tests run.
  If it is not set, the suit will pick a random node among the workers.

- `DRACPU_E2E_RESERVED_CPUS`: (optional) if set, it is meant to mirror the driver `reservedCPUs`
  setting. The value must be a linux cpuset string. The tests will ensure no containers
  consume these CPUs.
  NOTE: the Makefile defaults to "0" if not set, because a nonempty value is needed internally,
  and to empty on a lane, whose partitions replace it.
  NOTE: at the moment is not easy to autodetect this setting from the driver configuration.
  Future version may autodetect it.

- `DRACPU_E2E_CPU_DEVICE_MODE`: (optional): If set, change the behavior of the tests to assume
  the driver is configured with the corresponding device mode. Is meant to mirror the driver
  `cpuDeviceMode` setting.
  NOTE: the Makefile defaults to "grouped" if not set, because a nonempty value is needed internally.
  NOTE: Likewise the e2e tests, the Makefile also behaves differently depending on this setting.
  It is recommended to set it before to run targets like `make ci-manifests`.

- `DRACPU_E2E_ALLOCATOR`: (optional): If set, change the behavior of the tests to assume
  the driver is configured with the corresponding allocator. Is meant to enable external allocator testing
  and/or machine mode testing.
  NOTE: the Makefile defaults to "cpumanager" if not set, because a nonempty value is needed internally.

- `DRACPU_E2E_NODE_ALLOCATABLE_MAPPING`: (optional, default `false`, `true` on a lane): when `true`, *the Makefile*
  `ci-kind-setup` target deploys the driver with the
  `driverConfig.publishNodeAllocatableResourceMapping: true` option; any other value
  deploys without it. The cluster must have the `DRANodeAllocatableResources` feature
  gate; the stock kind configs enable it (they require a 1.37+ node image, which is the
  Makefile default).

- `DRACPU_E2E_FULL_PCPUS_ONLY`: (optional, default `false`): when `true`, *the Makefile*
  `ci-kind-setup` target deploys the driver with the `driverConfig.fullPhysicalCPUsOnly: true`
  option, and the specs labeled `whole-cores` run instead of skipping; they also skip on a node
  with no device whose cores all have two logical CPUs. `DRACPU_E2E_RESERVED_CPUS` must then be
  empty or hold whole cores, or the driver refuses to start. On a node whose cores have two logical
  CPUs the driver then refuses to prepare a claim for an amount that whole cores do not make up, as
  several other specs ask for, so run the whole-core specs alone there with
  `DRACPU_E2E_LABEL_FILTER=whole-cores`.

- `DRACPU_E2E_LANE`: (optional, default empty): the lane *the Makefile* `ci-kind-setup` target
  prepares; see [lanes](#lanes). Empty creates the cluster from `hack/ci/kind-ci.yaml` and installs
  the driver without CPU partitions.

- `DRACPU_E2E_NOSMT_PARTITION_CPUS`, `DRACPU_E2E_DEGRADED_PARTITION_CPUS` and
  `DRACPU_E2E_RESERVED_PARTITION_CPUS`: (optional, used on a lane): the cpusets of the `nosmt`,
  `degraded` and `reserved` partitions. `hack/ci/lane-setup.sh` computes each from the host where it
  is empty, and leaves it empty where the host lacks the shape.

- `DRACPU_E2E_REPORT`: (optional): a path, best absolute, where *the Makefile* `test-e2e` target
  writes the Ginkgo JSON report.

- `DRACPU_E2E_DUMP_RAW_LOGS`: (optional): if set to any value which is true-ish (e.g. `1`, `true`...)
  makes the tests which verify the contextual logging integrity dump the full raw captured logs
  before to run any actual test. Useful for troubleshooting and test fixing/tuning.
  NOTE: setting this value will make the test output significantly larger.

- `DRACPU_E2E_DAEMONSET_NAMESPACE`: (optional, default `kube-system`): the namespace where the
  dra-driver-cpu DaemonSet and its pods are deployed. Override this when the driver is installed
  in a namespace other than `kube-system`.

## how to run

The simplest way is from your terminal:

```bash
make test-e2e-kind
```

This creates a kind cluster from scratch, deploys the driver, and runs the full suite.
To run only specs labeled `metrics`, use:

```bash
DRACPU_E2E_LABEL_FILTER=metrics make test-e2e-kind
```

To run all specs except those labeled `negative` against an existing configured cluster:

```bash
DRACPU_E2E_LABEL_FILTER='!negative' make test-e2e
```

To run against an existing cluster with the driver already deployed:

```bash
make test-e2e
```

Note: `make test-e2e` does not build or load the test image; it only runs the Go e2e suite.
The `make test-e2e-kind` target builds and loads the image
automatically into a kind cluster, but for an arbitrary cluster the default `IMAGE_TEST` value
points at the local CI image name and will not be pullable. Either make sure the
test image is available to the cluster, or override the image explicitly:

```bash
DRACPU_E2E_TEST_IMAGE=<image> make test-e2e
```

### lanes

Specs that need more than the default install run on a lane, which
`make test-e2e-kind DRACPU_E2E_LANE=<lane>` prepares: `ci-kind-setup` takes
`hack/ci/kind-ci-<lane>.yaml` where it exists, and runs `hack/ci/lane-setup.sh <lane> pre` before it creates the cluster and
`hack/ci/lane-setup.sh <lane> post` after. Each lane's specs carry a Ginkgo label, and skip
where their lane did not prepare the cluster.

- No lane: the driver without CPU partitions, its devices owned by the claims, as the Prow jobs
  of this repository install it. Those jobs set none of the lane's variables, so they run this
  lane as they stand, and the specs of the other lanes skip there.
- `partitions` (label `partitions`): the driver with the profiles `e2e`, on the control plane and
  `worker`, and `e2e-wide`, on `worker2`, which `ci-kind-setup` labels with `dra.cpu/profile`.
  `e2e-wide`'s `default` entry sets `allowExclusive`, so `worker2` gives a request with
  `cpuConfig.exclusive: true` CPUs of its own, and the other nodes refuse it.
  Both hold the `default` partition and, where the host has a core without an online sibling,
  a partition `nosmt` of one such core. `e2e` also holds, where the host has a core with an online
  sibling, a partition `degraded` under `smt: false` of one thread of such a core, whose devices the
  driver publishes tainted, the lane setting `taintDegradedPartitions`, and a partition `reserved`
  of one whole core. `lane-setup.sh pre` places `degraded` before `reserved`, leaves at least two
  CPUs to `default`. Each kubelet reserves the CPUs outside its profile's `default` partition, as
  the driver requires before it publishes devices. The
  driver publishes the node allocatable mapping, which only the `default` partition's shared
  devices carry. Where `nosmt` is declared, a third profile, `e2e-grown`, gives its CPUs to a
  `reserved` partition `grown`; no node runs it until a spec moves `worker2` to it. The specs of
  the `nosmt` partition skip where the host has no core without an online sibling, which is any
  host whose cores all run two threads. The specs that restart containerd in a node run
  `docker exec` on the kind node's container, and skip where `docker` cannot reach it. The driver's
  pod carries `e2e.dra.cpu/tolerate-missing-nri-plugins: "true"`, the annotation the lane's
  containerd lets a pod start with while a plugin it requires is missing.
- `gates-off` (label `gates-off`): the `partitions` lane's install on a cluster from
  `hack/ci/kind-ci-gates-off.yaml`, which turns `DRANodeAllocatableResources` and `DRADeviceTaints`
  off in every component. Its API server drops the node allocatable mapping and the degraded
  partition's taint from the driver's ResourceSlices, and the specs check what the driver does
  then. Each spec skips where the API server reports its gate on.

`.github/workflows/e2e-lanes.yaml` runs the `partitions` lane on an x64 and an arm64 runner and the
`gates-off` lane on the x64 runner, and fails when a spec the runner's host can run was skipped.

### troubleshooting

- **Kubelet fails with `unknown service runtime.v1.RuntimeService`**

  The real error is in the containerd logs: `failed to create fsnotify watcher: too many open files`.
  The host's `fs.inotify.max_user_instances` (default 128) is exhausted.

  Fix: `sudo sysctl fs.inotify.max_user_instances=1024`, then recreate the cluster.

- **`kind load docker-image` fails with `no unpack platforms defined`**

  Happens with fuse-overlayfs storage driver and kind < v0.32.0
  ([kind#3945](https://github.com/kubernetes-sigs/kind/issues/3945).

  Fix: upgrade kind and rebuild the node image:

  ```bash
  go install sigs.k8s.io/kind@v0.32.0
  docker rmi kindest/node:<version>
  ```

- **Pods fail with `openat2 .../cpuset.cpus: no such file or directory`**

  When running kind with rootless Docker, the `cpuset` cgroup controller is not
  delegated to user sessions by default. The NRI hook sets `cpuset.cpus` on every
  container, so all pods (including CoreDNS) crash on startup.

  Verify by checking inside a kind node:

  ```bash
  docker exec <kind-node> cat /sys/fs/cgroup/cgroup.controllers
  # If "cpuset" is missing from the output, this is the issue.
  ```

  Fix:

  ```bash
  sudo mkdir -p /etc/systemd/system/user@.service.d
  sudo tee /etc/systemd/system/user@.service.d/delegate-cpuset.conf <<EOF
  [Service]
  Delegate=cpuset cpu io memory pids
  EOF
  sudo systemctl daemon-reload
  ```

  Then recreate the kind cluster.
