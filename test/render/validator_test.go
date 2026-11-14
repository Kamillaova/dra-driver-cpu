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
// It also judges a whole rendered chart for the admission policy that keeps the
// driver's service account to its own node's events, and the verbs it confines.
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
)

var (
	manifest      = flag.String("manifest", "", "path to a rendered DaemonSet")
	expectedRoot  = flag.String("expected-root", "", "the kubelet root the mounts and the flag must derive from")
	expectFlag    = flag.Bool("expect-root-flag", false, "whether the chart should pass "+rootFlag)
	checkOverride = flag.Bool("check-hostname-override", false, "whether to judge how the node name reaches the driver")
	wantStrategy  = flag.String("expected-update-strategy", "{}", "the spec.updateStrategy the chart must render, as JSON; {} for none")

	chart         = flag.String("chart", "", "path to a whole rendered chart")
	expectPolicy  = flag.Bool("expect-events-policy", true, "whether the chart should render the events admission policy and the ClusterRole")
	releaseNS     = flag.String("release-namespace", "", "the namespace the chart was rendered for")
	chartFullname = flag.String("fullname", "", "the name the chart gives the release's objects")
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

// TestRenderedChart judges the events admission policy. Its CEL is the API
// server's to check; this pins what helm decides: whether the policy, its
// binding and the verbs they confine render at all, and which account and
// objects they name.
func TestRenderedChart(t *testing.T) {
	if *chart == "" {
		t.Skip("no -chart given; run this through `make helm-render-check`")
	}
	docs, err := readDocuments(*chart)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkEventsPolicy(docs, *expectPolicy, *releaseNS, *chartFullname); err != nil {
		t.Fatal(err)
	}
	chartValidated = true
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

// checkEventsPolicy pins the policy to the release's own service account: a
// policy matching any other account would refuse a kubelet's or a
// controller's events, and one matching none would confine nothing.
func checkEventsPolicy(docs []document, want bool, namespace, fullname string) error {
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
	if len(policies) != 1 || len(bindings) != 1 || len(roles) != 1 {
		return fmt.Errorf("the chart renders %d policies, %d bindings and %d ClusterRoles, want one of each", len(policies), len(bindings), len(roles))
	}

	var errs []error
	policy, binding, role := policies[0], bindings[0], roles[0]
	name := fullname + "-events"
	if policy.Name != name {
		errs = append(errs, fmt.Errorf("the policy is named %q, want %q", policy.Name, name))
	}
	if fp := policy.Spec.FailurePolicy; fp == nil || *fp != admissionregistrationv1.Fail {
		errs = append(errs, fmt.Errorf("the policy's failurePolicy is not %q", admissionregistrationv1.Fail))
	}
	wantRules := []admissionregistrationv1.NamedRuleWithOperations{{
		RuleWithOperations: admissionregistrationv1.RuleWithOperations{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"events"},
			},
		},
	}}
	if mc := policy.Spec.MatchConstraints; mc == nil || !reflect.DeepEqual(mc.ResourceRules, wantRules) {
		errs = append(errs, fmt.Errorf("the policy matches %v, want %v", policy.Spec.MatchConstraints, wantRules))
	}
	account := fmt.Sprintf(`request.userInfo.username == "system:serviceaccount:%s:%s"`, namespace, fullname)
	if mcs := policy.Spec.MatchConditions; len(mcs) != 1 || mcs[0].Expression != account {
		errs = append(errs, fmt.Errorf("the policy's match conditions are %v, want one: %s", mcs, account))
	}
	if len(policy.Spec.Validations) == 0 {
		errs = append(errs, errors.New("the policy has no validations"))
	}
	if binding.Name != name || binding.Spec.PolicyName != policy.Name {
		errs = append(errs, fmt.Errorf("the binding %q binds policy %q, want %q binding %q", binding.Name, binding.Spec.PolicyName, name, policy.Name))
	}
	if !reflect.DeepEqual(binding.Spec.ValidationActions, []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}) {
		errs = append(errs, fmt.Errorf("the binding's validationActions are %v, want [Deny]", binding.Spec.ValidationActions))
	}
	if role.Name != fullname {
		errs = append(errs, fmt.Errorf("the ClusterRole is named %q, want %q", role.Name, fullname))
	}
	var eventVerbs [][]string
	for _, rule := range role.Rules {
		if slices.Contains(rule.Resources, "events") {
			eventVerbs = append(eventVerbs, rule.Verbs)
			if !reflect.DeepEqual(rule.APIGroups, []string{""}) {
				errs = append(errs, fmt.Errorf("the ClusterRole grants events in API groups %v, want the core group alone", rule.APIGroups))
			}
		}
	}
	if !reflect.DeepEqual(eventVerbs, [][]string{{"create", "patch"}}) {
		errs = append(errs, fmt.Errorf("the ClusterRole grants events the verbs %v, want one rule with create and patch, which the policy confines", eventVerbs))
	}
	return errors.Join(errs...)
}
