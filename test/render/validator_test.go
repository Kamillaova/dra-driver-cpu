/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package render judges one already-rendered DaemonSet: the kubelet root has to
// reach both mount paths and the driver flag as the same directory, or the driver
// registers where the kubelet is not watching, which is issue #231. The node
// name the driver registers under has to come from the Node object, not the
// kernel hostname, or ResourceSlice publishing fails to find the node owner.
// It also judges a whole rendered chart for the admission policies that keep the
// driver's service account to its own node's objects, and the verbs they confine,
// or for the DeviceClasses it renders.
// Rendering and the expected values come from hack/ci/helm-render-check.sh.
package render

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	driverContainer   = "dracpu"
	pluginsVolume     = "device-plugin"
	registryVolume    = "plugin-registry"
	rootFlag          = "--kubelet-root-dir"
	rootFlagShort     = "-kubelet-root-dir"
	nodeNameEnv       = "NODE_NAME"
	nodeNameFieldPath = "spec.nodeName"
	overrideFlag      = "--hostname-override"
	overrideFlagShort = "-hostname-override"
	defaultClass      = "dra.cpu"
)

var (
	manifest      = flag.String("manifest", "", "path to a rendered DaemonSet")
	expectedRoot  = flag.String("expected-root", "", "the kubelet root the mounts and the flag must derive from")
	expectFlag    = flag.Bool("expect-root-flag", false, "whether the chart should pass "+rootFlag)
	checkOverride = flag.Bool("check-hostname-override", false, "whether to judge how the node name reaches the driver")
	wantStrategy  = flag.String("expected-update-strategy", "{}", "the spec.updateStrategy the chart must render, as JSON; {} for none")
	threadedMode  = flag.String("expected-threaded-mode", "false", "the cgroupThreadedMode whose host access the chart must render: false, create or place")

	chart         = flag.String("chart", "", "path to a whole rendered chart")
	expectPolicy  = flag.Bool("expect-admission-policies", true, "whether the chart should render the admission policies and the ClusterRole")
	releaseNS     = flag.String("release-namespace", "", "the namespace the chart was rendered for")
	chartFullname = flag.String("fullname", "", "the name the chart gives the release's objects")
	checkClasses  = flag.Bool("check-device-classes", false, "whether to judge the DeviceClasses the chart renders")
	wantClasses   = flag.String("expected-device-classes", "", "the DeviceClass names the chart must render, comma-separated in name order")
)

// validated says the checks below ran to the end. `go test -run` exits 0 when
// its pattern matches nothing, and a test that skips exits 0 too, so a renamed,
// deleted or short-circuited test would otherwise leave every caller green with
// nothing asserted.
var validated, chartValidated bool

func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 && *manifest != "" && !validated {
		fmt.Fprintf(os.Stderr, "no test read %s: the validator did not run\n", *manifest)
		code = 1
	}
	if code == 0 && *chart != "" && !chartValidated {
		fmt.Fprintf(os.Stderr, "no test read %s: the validator did not run\n", *chart)
		code = 1
	}
	os.Exit(code)
}

func TestRenderedDaemonSet(t *testing.T) {
	if *manifest == "" {
		t.Skip("no -manifest given; run this through `make helm-render-check`")
	}
	ds, err := readDaemonSet(*manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRoot(ds, *expectedRoot, *expectFlag); err != nil {
		t.Fatal(err)
	}
	c, err := driver(ds)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		name  string
		probe *corev1.Probe
		path  string
	}{
		{name: "liveness", probe: c.LivenessProbe, path: "/livez"},
		{name: "readiness", probe: c.ReadinessProbe, path: "/healthz"},
	} {
		if p.probe == nil || p.probe.HTTPGet == nil {
			t.Fatalf("%s probe must use HTTP", p.name)
		}
		if p.probe.HTTPGet.Path != p.path {
			t.Fatalf("%s probe path = %q, want %s", p.name, p.probe.HTTPGet.Path, p.path)
		}
	}
	if err := checkUpdateStrategy(ds, *wantStrategy); err != nil {
		t.Fatal(err)
	}
	if err := checkThreadedAccess(ds, *threadedMode); err != nil {
		t.Fatal(err)
	}
	if *checkOverride {
		if err := checkNodeName(ds); err != nil {
			t.Fatal(err)
		}
	}
	validated = true
}

// readDaemonSet takes the one DaemonSet out of a rendered stream. Nothing stops
// a template file holding more than one document, and reading only the first
// would leave a second one unjudged.
//
// Documents are judged before they are given a type. Decoding straight into a
// DaemonSet drops the fields that type does not have, so a document of
// unrelated keys would arrive empty and read as a comment.
func readDaemonSet(p string) (*appsv1.DaemonSet, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	want := appsv1.SchemeGroupVersion.String()
	var found []*appsv1.DaemonSet
	r := utilyaml.NewYAMLReader(bufio.NewReader(f))
	for i := 0; ; i++ {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		if commentOnly(doc) {
			continue
		}
		raw, err := utilyaml.ToJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		var tm metav1.TypeMeta
		if err := json.Unmarshal(raw, &tm); err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		if tm.APIVersion != want || tm.Kind != "DaemonSet" {
			return nil, fmt.Errorf("%s document %d: want %s DaemonSet, got apiVersion %q and kind %q",
				p, i, want, tm.APIVersion, tm.Kind)
		}
		ds := &appsv1.DaemonSet{}
		if err := json.Unmarshal(raw, ds); err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		found = append(found, ds)
	}
	if len(found) != 1 {
		return nil, fmt.Errorf("%s holds %d DaemonSets, want one", p, len(found))
	}
	return found[0], nil
}

// commentOnly reports whether the document holds nothing but comments. The
// template's own licence header renders as one. An explicit null or {} is not
// one: Helm keeps both, so they have to face the check below.
func commentOnly(doc []byte) bool {
	for line := range bytes.SplitSeq(doc, []byte("\n")) {
		if t := bytes.TrimSpace(line); len(t) > 0 && !bytes.HasPrefix(t, []byte("#")) {
			return false
		}
	}
	return true
}

func checkRoot(ds *appsv1.DaemonSet, root string, wantFlag bool) error {
	c, err := driver(ds)
	if err != nil {
		return err
	}
	errs := []error{
		checkBinding(ds, c, pluginsVolume, path.Join(root, "plugins")),
		checkBinding(ds, c, registryVolume, path.Join(root, "plugins_registry")),
		checkFlag(c, root, wantFlag),
	}
	return errors.Join(errs...)
}

func driver(ds *appsv1.DaemonSet) (*corev1.Container, error) {
	var found []*corev1.Container
	for i := range ds.Spec.Template.Spec.Containers {
		if c := &ds.Spec.Template.Spec.Containers[i]; c.Name == driverContainer {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		return nil, fmt.Errorf("%d containers named %q, want one", len(found), driverContainer)
	}
	return found[0], nil
}

// checkBinding reads the mount and its volume as one: the driver writes through
// the mount and the kubelet reads the host path behind it.
func checkBinding(ds *appsv1.DaemonSet, c *corev1.Container, name, want string) error {
	var mounts []corev1.VolumeMount
	for _, m := range c.VolumeMounts {
		if m.Name == name {
			mounts = append(mounts, m)
		}
	}
	if len(mounts) != 1 {
		return fmt.Errorf("%d mounts named %q, want one", len(mounts), name)
	}
	m := mounts[0]
	var errs []error
	if m.MountPath != want {
		errs = append(errs, fmt.Errorf("mount %q is at %q, want %q", name, m.MountPath, want))
	}
	// The sockets are created here, so a partial view of the directory leaves the
	// kubelet watching a different one.
	if m.ReadOnly {
		errs = append(errs, fmt.Errorf("mount %q is read-only", name))
	}
	if m.SubPath != "" || m.SubPathExpr != "" {
		errs = append(errs, fmt.Errorf("mount %q carries a subPath", name))
	}

	var volumes []corev1.Volume
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.Name == name {
			volumes = append(volumes, v)
		}
	}
	if len(volumes) != 1 {
		return errors.Join(append(errs, fmt.Errorf("%d volumes named %q, want one", len(volumes), name))...)
	}
	host := volumes[0].HostPath
	if host == nil {
		return errors.Join(append(errs, fmt.Errorf("volume %q is not a hostPath", name))...)
	}
	if host.Path != want {
		errs = append(errs, fmt.Errorf("volume %q points at %q, want %q", name, host.Path, want))
	}
	// The kubelet creates both directories under its root at startup, so a
	// missing one means the root is wrong. Directory refuses to start the pod
	// there, where an unset type would mount an empty directory instead.
	if host.Type == nil || *host.Type != corev1.HostPathDirectory {
		errs = append(errs, fmt.Errorf("volume %q has hostPath type %s, want %q", name, typeOf(host), corev1.HostPathDirectory))
	}
	return errors.Join(errs...)
}

func typeOf(host *corev1.HostPathVolumeSource) string {
	if host.Type == nil {
		return "unset"
	}
	return fmt.Sprintf("%q", *host.Type)
}

// checkFlag looks for the argument the chart manages. At the default the chart
// says nothing, leaving the root to the driver's own.
func checkFlag(c *corev1.Container, root string, want bool) error {
	var found []string
	for _, arg := range append(append([]string{}, c.Command...), c.Args...) {
		// The driver parses with the flag package, which takes one dash or two
		// and rejects more, so only those two spellings name the root. Which of
		// them the chart used is judged below.
		if name, _, _ := strings.Cut(arg, "="); name == rootFlag || name == rootFlagShort {
			found = append(found, arg)
		}
	}
	if !want {
		if len(found) != 0 {
			return fmt.Errorf("the chart passes %v at the default root, want none", found)
		}
		return nil
	}
	if len(found) != 1 {
		return fmt.Errorf("%d %s arguments, want one", len(found), rootFlag)
	}
	if got := found[0]; got != rootFlag+"="+root {
		return fmt.Errorf("the chart passes %q, want %q", got, rootFlag+"="+root)
	}
	return nil
}

// checkNodeName judges how the chart pins the node the driver registers under.
// The kernel hostname may differ from the Node object name (e.g. it is an IP
// address), which breaks ResourceSlice publishing, so the chart has to supply
// the Node name through the NODE_NAME downward API env var. The removed
// --hostname-override flag must not be passed.
func checkNodeName(ds *appsv1.DaemonSet) error {
	c, err := driver(ds)
	if err != nil {
		return err
	}
	for _, arg := range append(append([]string{}, c.Command...), c.Args...) {
		if name, _, _ := strings.Cut(arg, "="); name == overrideFlag || name == overrideFlagShort {
			return fmt.Errorf("the chart passes %q, but the flag was removed", arg)
		}
	}
	return checkNodeNameEnv(c)
}

// checkNodeNameEnv pins the downward API source of the node name: a missing or
// renamed env leaves the driver registering under the kernel hostname.
func checkNodeNameEnv(c *corev1.Container) error {
	var found []corev1.EnvVar
	for _, e := range c.Env {
		if e.Name == nodeNameEnv {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		return fmt.Errorf("%d env vars named %q, want one", len(found), nodeNameEnv)
	}
	e := found[0]
	if e.ValueFrom == nil || e.ValueFrom.FieldRef == nil {
		return fmt.Errorf("env %q is not a fieldRef", nodeNameEnv)
	}
	if want := nodeNameFieldPath; e.ValueFrom.FieldRef.FieldPath != want {
		return fmt.Errorf("env %q comes from %q, want %q", nodeNameEnv, e.ValueFrom.FieldRef.FieldPath, want)
	}
	return nil
}

// checkUpdateStrategy compares the strategy as decoded objects, so a case may
// list the fields in any order. The expected value is decoded strictly, since a
// misspelled field would otherwise read as unset and pass against a chart that
// renders none.
func checkUpdateStrategy(ds *appsv1.DaemonSet, want string) error {
	var expected appsv1.DaemonSetUpdateStrategy
	dec := json.NewDecoder(strings.NewReader(want))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&expected); err != nil {
		return fmt.Errorf("parse -expected-update-strategy %q: %w", want, err)
	}
	if reflect.DeepEqual(ds.Spec.UpdateStrategy, expected) {
		return nil
	}
	got, err := json.Marshal(ds.Spec.UpdateStrategy)
	if err != nil {
		return fmt.Errorf("encode the rendered updateStrategy: %w", err)
	}
	return fmt.Errorf("updateStrategy = %s, want %s", got, want)
}

// checkThreadedAccess judges the host access cgroupThreadedMode needs: under
// create and place the host's cgroup2 tree read-write at the path the driver
// writes and the host's pid namespace, under place also the capability that
// moves a process into a group of other CPUs and the two that enter PID 1's
// cgroup namespace, and under false none of it.
func checkThreadedAccess(ds *appsv1.DaemonSet, mode string) error {
	if mode != "false" && mode != "create" && mode != "place" {
		return fmt.Errorf("-expected-threaded-mode %q: want false, create or place", mode)
	}
	c, err := driver(ds)
	if err != nil {
		return err
	}
	threaded := mode != "false"
	var errs []error
	if ds.Spec.Template.Spec.HostPID != threaded {
		errs = append(errs, fmt.Errorf("hostPID = %t under %s, want %t", ds.Spec.Template.Spec.HostPID, mode, threaded))
	}
	var mounts []corev1.VolumeMount
	for _, m := range c.VolumeMounts {
		if m.MountPath == "/sys/fs/cgroup" {
			mounts = append(mounts, m)
		}
	}
	switch {
	case !threaded && len(mounts) != 0:
		errs = append(errs, fmt.Errorf("%d mounts at /sys/fs/cgroup under false, want none", len(mounts)))
	case threaded && len(mounts) != 1:
		errs = append(errs, fmt.Errorf("%d mounts at /sys/fs/cgroup under %s, want one", len(mounts), mode))
	case threaded:
		if err := checkCgroupMount(ds, mounts[0]); err != nil {
			errs = append(errs, err)
		}
	}
	var added []corev1.Capability
	if sc := c.SecurityContext; sc != nil && sc.Capabilities != nil {
		added = sc.Capabilities.Add
	}
	var want []corev1.Capability
	if mode == "place" {
		want = []corev1.Capability{"SYS_ADMIN", "SYS_NICE", "SYS_PTRACE"}
	}
	if !slices.Equal(added, want) {
		errs = append(errs, fmt.Errorf("capabilities added under %s = %v, want %v", mode, added, want))
	}
	if sc := c.SecurityContext; sc != nil && sc.Privileged != nil && *sc.Privileged {
		errs = append(errs, errors.New("the driver's container is privileged"))
	}
	return errors.Join(errs...)
}

func checkCgroupMount(ds *appsv1.DaemonSet, m corev1.VolumeMount) error {
	if m.ReadOnly {
		return fmt.Errorf("mount %q at /sys/fs/cgroup is read-only", m.Name)
	}
	if m.SubPath != "" || m.SubPathExpr != "" {
		return fmt.Errorf("mount %q at /sys/fs/cgroup carries a subPath", m.Name)
	}
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.Name != m.Name {
			continue
		}
		if v.HostPath == nil || v.HostPath.Path != "/sys/fs/cgroup" {
			return fmt.Errorf("volume %q is not the host's /sys/fs/cgroup", m.Name)
		}
		if v.HostPath.Type == nil || *v.HostPath.Type != corev1.HostPathDirectory {
			return fmt.Errorf("volume %q has hostPath type %s, want %q", m.Name, typeOf(v.HostPath), corev1.HostPathDirectory)
		}
		return nil
	}
	return fmt.Errorf("no volume %q", m.Name)
}

// TestRenderedChart judges the admission policies, or the DeviceClasses. Their CEL is the API
// server's to check; this pins what helm decides: whether the policies, their
// bindings and the verbs they confine render at all, and which account and
// objects they name.
func TestRenderedChart(t *testing.T) {
	if *chart == "" {
		t.Skip("no -chart given; run this through `make helm-render-check`")
	}
	docs, err := readDocuments(*chart)
	if err != nil {
		t.Fatal(err)
	}
	if *checkClasses {
		if err := checkDeviceClasses(docs, *wantClasses); err != nil {
			t.Fatal(err)
		}
	} else if err := checkAdmissionPolicies(docs, *expectPolicy, *releaseNS, *chartFullname); err != nil {
		t.Fatal(err)
	}
	chartValidated = true
}

// checkDeviceClasses pins each class's selector: dra.cpu matches a device of
// the partition "default" or one without the partition attribute, as an
// upstream driver publishes it, and dra.cpu.<name> matches <name> alone. has()
// keeps either from failing on a device without the attribute.
func checkDeviceClasses(docs []document, want string) error {
	classes, err := decodeAll[resourceapi.DeviceClass](docs, resourceapi.SchemeGroupVersion.String(), "DeviceClass")
	if err != nil {
		return err
	}
	var names []string
	for _, class := range classes {
		names = append(names, class.Name)
	}
	var wantNames []string
	if want != "" {
		wantNames = strings.Split(want, ",")
	}
	if !slices.Equal(names, wantNames) {
		return fmt.Errorf("DeviceClasses %q, want %q", names, wantNames)
	}
	for _, class := range classes {
		wantExpression := `device.driver == "dra.cpu" && (!has(device.attributes["dra.cpu"].partition) || device.attributes["dra.cpu"].partition == "default")`
		if partition, named := strings.CutPrefix(class.Name, defaultClass+"."); named {
			wantExpression = `device.driver == "dra.cpu" && has(device.attributes["dra.cpu"].partition) && device.attributes["dra.cpu"].partition == "` + partition + `"`
		}
		if len(class.Spec.Selectors) != 1 || class.Spec.Selectors[0].CEL == nil {
			return fmt.Errorf("DeviceClass %s has %d selectors, want one CEL selector", class.Name, len(class.Spec.Selectors))
		}
		if got := class.Spec.Selectors[0].CEL.Expression; got != wantExpression {
			return fmt.Errorf("DeviceClass %s selects %s, want %s", class.Name, got, wantExpression)
		}
	}
	return nil
}

type document struct {
	typeMeta metav1.TypeMeta
	raw      []byte
}

func readDocuments(p string) ([]document, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var docs []document
	r := utilyaml.NewYAMLReader(bufio.NewReader(f))
	for i := 0; ; i++ {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		if commentOnly(doc) {
			continue
		}
		raw, err := utilyaml.ToJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		var tm metav1.TypeMeta
		if err := json.Unmarshal(raw, &tm); err != nil {
			return nil, fmt.Errorf("%s document %d: %w", p, i, err)
		}
		docs = append(docs, document{typeMeta: tm, raw: raw})
	}
}

// decodeAll decodes every document of one kind and API version.
func decodeAll[T any](docs []document, apiVersion, kind string) ([]T, error) {
	var found []T
	for _, d := range docs {
		if d.typeMeta.Kind != kind {
			continue
		}
		if d.typeMeta.APIVersion != apiVersion {
			return nil, fmt.Errorf("%s has apiVersion %q, want %q", kind, d.typeMeta.APIVersion, apiVersion)
		}
		var obj T
		if err := json.Unmarshal(d.raw, &obj); err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		found = append(found, obj)
	}
	return found, nil
}

// admissionPolicies are the policies the chart renders, one per resource the
// driver's service account writes, by the suffix each adds to the release's name.
var admissionPolicies = map[string]admissionregistrationv1.RuleWithOperations{
	"events": {
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"events"}},
	},
	"node-status": {
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"nodes/status"}},
	},
	"resourceslices": {
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update, admissionregistrationv1.Delete},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{"resource.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"resourceslices"}},
	},
	"resourceclaim-status": {
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{"resource.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"resourceclaims/status"}},
	},
}

// confinedVerbs are the ClusterRole's grants that only the policies keep to
// the driver's own node.
var confinedVerbs = map[string][]string{
	"events":       {"create", "patch"},
	"nodes/status": {"patch"},
}

// checkAdmissionPolicies pins each policy to the release's own service
// account: a policy matching any other account would refuse a kubelet's or a
// controller's writes, and one matching none would confine nothing.
func checkAdmissionPolicies(docs []document, want bool, namespace, fullname string) error {
	admissionVersion := admissionregistrationv1.SchemeGroupVersion.String()
	policies, err := decodeAll[admissionregistrationv1.ValidatingAdmissionPolicy](docs, admissionVersion, "ValidatingAdmissionPolicy")
	if err != nil {
		return err
	}
	bindings, err := decodeAll[admissionregistrationv1.ValidatingAdmissionPolicyBinding](docs, admissionVersion, "ValidatingAdmissionPolicyBinding")
	if err != nil {
		return err
	}
	roles, err := decodeAll[rbacv1.ClusterRole](docs, rbacv1.SchemeGroupVersion.String(), "ClusterRole")
	if err != nil {
		return err
	}
	if !want {
		if len(policies)+len(bindings)+len(roles) != 0 {
			return fmt.Errorf("the chart renders %d policies, %d bindings and %d ClusterRoles, want none", len(policies), len(bindings), len(roles))
		}
		return nil
	}
	if len(policies) != len(admissionPolicies) || len(bindings) != len(admissionPolicies) || len(roles) != 1 {
		return fmt.Errorf("the chart renders %d policies, %d bindings and %d ClusterRoles, want %d, %d and 1",
			len(policies), len(bindings), len(roles), len(admissionPolicies), len(admissionPolicies))
	}

	var errs []error
	account := fmt.Sprintf(`request.userInfo.username == "system:serviceaccount:%s:%s"`, namespace, fullname)
	byName := make(map[string]admissionregistrationv1.ValidatingAdmissionPolicy, len(policies))
	for _, p := range policies {
		byName[p.Name] = p
	}
	bindingByName := make(map[string]admissionregistrationv1.ValidatingAdmissionPolicyBinding, len(bindings))
	for _, b := range bindings {
		bindingByName[b.Name] = b
	}
	for _, suffix := range slices.Sorted(maps.Keys(admissionPolicies)) {
		name := fullname + "-" + suffix
		policy, ok := byName[name]
		if !ok {
			errs = append(errs, fmt.Errorf("no policy is named %q", name))
			continue
		}
		if fp := policy.Spec.FailurePolicy; fp == nil || *fp != admissionregistrationv1.Fail {
			errs = append(errs, fmt.Errorf("policy %q: failurePolicy is not %q", name, admissionregistrationv1.Fail))
		}
		wantRules := []admissionregistrationv1.NamedRuleWithOperations{{RuleWithOperations: admissionPolicies[suffix]}}
		if mc := policy.Spec.MatchConstraints; mc == nil || !reflect.DeepEqual(mc.ResourceRules, wantRules) {
			errs = append(errs, fmt.Errorf("policy %q matches %v, want %v", name, policy.Spec.MatchConstraints, wantRules))
		}
		if mcs := policy.Spec.MatchConditions; len(mcs) != 1 || mcs[0].Expression != account {
			errs = append(errs, fmt.Errorf("policy %q: match conditions are %v, want one: %s", name, mcs, account))
		}
		if len(policy.Spec.Validations) == 0 {
			errs = append(errs, fmt.Errorf("policy %q has no validations", name))
		}
		binding, ok := bindingByName[name]
		if !ok || binding.Spec.PolicyName != name {
			errs = append(errs, fmt.Errorf("no binding %q binds policy %q", name, name))
		} else if !reflect.DeepEqual(binding.Spec.ValidationActions, []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}) {
			errs = append(errs, fmt.Errorf("binding %q: validationActions are %v, want [Deny]", name, binding.Spec.ValidationActions))
		}
	}

	role := roles[0]
	if role.Name != fullname {
		errs = append(errs, fmt.Errorf("the ClusterRole is named %q, want %q", role.Name, fullname))
	}
	for _, resource := range slices.Sorted(maps.Keys(confinedVerbs)) {
		var verbs [][]string
		for _, rule := range role.Rules {
			if slices.Contains(rule.Resources, resource) {
				verbs = append(verbs, rule.Verbs)
				if !reflect.DeepEqual(rule.APIGroups, []string{""}) {
					errs = append(errs, fmt.Errorf("the ClusterRole grants %s in API groups %v, want the core group alone", resource, rule.APIGroups))
				}
			}
		}
		if want := [][]string{confinedVerbs[resource]}; !reflect.DeepEqual(verbs, want) {
			errs = append(errs, fmt.Errorf("the ClusterRole grants %s the verbs %v, want one rule with %v, which a policy confines", resource, verbs, want[0]))
		}
	}
	var nodeVerbs [][]string
	for _, rule := range role.Rules {
		if slices.Contains(rule.Resources, "nodes") {
			nodeVerbs = append(nodeVerbs, rule.Verbs)
		}
	}
	if want := [][]string{{"get", "list", "watch"}}; !reflect.DeepEqual(nodeVerbs, want) {
		errs = append(errs, fmt.Errorf("the ClusterRole grants nodes the verbs %v, want one rule with %v, which the reservation check's Node watch needs", nodeVerbs, want[0]))
	}
	return errors.Join(errs...)
}
