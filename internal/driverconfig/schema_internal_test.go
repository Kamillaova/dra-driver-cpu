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

// Internal package test: has direct access to the unexported schema metadata.
package driverconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// TestGenerateDriverConfigSchema_CoversAllFields: every Config json field is
// either excluded on purpose (schemaExcludedFields) or present in the
// generated schema's properties.
func TestGenerateDriverConfigSchema_CoversAllFields(t *testing.T) {
	out, err := GenerateDriverConfigSchema()
	if err != nil {
		t.Fatalf("GenerateDriverConfigSchema() error: %v", err)
	}

	var doc struct {
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshaling generated schema: %v", err)
	}

	typ := reflect.TypeFor[Config]()
	for field := range typ.Fields() {
		jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if jsonName == "" || jsonName == "-" {
			continue
		}
		_, present := doc.Properties[jsonName]
		if _, excluded := schemaExcludedFields[jsonName]; excluded {
			if present {
				t.Errorf("Config field %q (json key %q) is marked excluded but appears in the generated schema", field.Name, jsonName)
			}
			continue
		}
		if !present {
			t.Errorf("Config field %q (json key %q) is missing from the generated schema", field.Name, jsonName)
		}
	}
}

// TestConfigDeclaresExplicitJSONNames pins the shape canonicalConfigKeys reads.
//
// That helper takes direct fields and explicit tag names, which is all Config
// has. encoding/json resolves more than that -- it promotes an embedded
// struct's fields, matches an untagged field by its Go name, and reads
// `json:"-,"` as a field literally named "-" rather than an ignored one -- so a
// field of any of those shapes would be settable through a config file while
// the helper had never heard of it, which is the gap it exists to close.
//
// Enforced here rather than reimplemented there: a partial copy of that
// resolution would have to be kept in step with the standard library by hand,
// and this fails at build time the moment Config grows a shape it does not
// cover.
func TestConfigDeclaresExplicitJSONNames(t *testing.T) {
	// Seeded rather than empty: apiVersion is not a Config field, so nothing in
	// the loop would collide with it, but buildConfMap reads and strips it before
	// the decoder runs. A field claiming that name would pass every check here
	// and then never be settable from a file at all.
	byName := map[string]string{"apiVersion": "the schema property buildConfMap strips"}
	for field := range reflect.TypeFor[Config]().Fields() {
		if field.Anonymous {
			t.Errorf("field %q is embedded; canonicalConfigKeys reads direct fields only, "+
				"so the fields promoted out of it would not be in the canonical set", field.Name)
			continue
		}
		if field.PkgPath != "" {
			continue // unexported, so the decoder cannot reach it either
		}
		tag, tagged := field.Tag.Lookup("json")
		if !tagged {
			t.Errorf("field %q has no json tag; the decoder would match it by its Go name, "+
				"which is not what canonicalConfigKeys collects", field.Name)
			continue
		}
		name, _, hasOptions := strings.Cut(tag, ",")
		if name == "-" {
			if hasOptions {
				t.Errorf(`field %q is tagged json:"-,", which names it "-" rather than `+
					`ignoring it; canonicalConfigKeys reads it as ignored`, field.Name)
			}
			continue
		}
		if name == "" {
			t.Errorf("field %q has an empty json name; the decoder would match it by its "+
				"Go name, which is not what canonicalConfigKeys collects", field.Name)
			continue
		}
		for other, otherField := range byName {
			if strings.EqualFold(name, other) {
				t.Errorf("json names %q (%s) and %q (%s) fold onto each other, so one of them "+
					"could never be spelled canonically", name, field.Name, other, otherField)
			}
		}
		byName[name] = field.Name
	}
}

// The chart owns the kubelet root, since its hostPath mounts render from the
// same value, so it stays out of the schema like bindAddress and exposePCIeRoots.
func TestGenerateDriverConfigSchema_ExcludesKubeletRootDir(t *testing.T) {
	out, err := GenerateDriverConfigSchema()
	if err != nil {
		t.Fatalf("GenerateDriverConfigSchema: %v", err)
	}

	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(out, &schema); err != nil {
		t.Fatalf("unmarshal generated schema: %v\n%s", err, out)
	}

	if _, ok := schema.Properties["kubeletRootDir"]; ok {
		t.Errorf("kubeletRootDir must not appear in the generated schema; got:\n%s", out)
	}
	if want := "use the chart's top-level kubeletRootDir value, or --kubelet-root-dir when running the binary directly"; schemaExcludedFields["kubeletRootDir"] != want {
		t.Errorf("kubeletRootDir exclusion hint = %q, want %q", schemaExcludedFields["kubeletRootDir"], want)
	}
}

// TestGenerateDriverConfigSchema_PartitionItemConstraints: a partition's role
// and thread-arity expectation are constrained inside the list item, where the
// Go types say only "string" and "object".
func TestGenerateDriverConfigSchema_PartitionItemConstraints(t *testing.T) {
	out, err := GenerateDriverConfigSchema()
	if err != nil {
		t.Fatalf("GenerateDriverConfigSchema() error: %v", err)
	}

	var doc struct {
		Properties struct {
			CPUPartitions struct {
				Items struct {
					Required   []string `json:"required"`
					Properties struct {
						Role struct {
							Enum []string `json:"enum"`
						} `json:"role"`
						SMT struct {
							OneOf []struct {
								Type    string   `json:"type"`
								Minimum *float64 `json:"minimum"`
							} `json:"oneOf"`
						} `json:"smt"`
					} `json:"properties"`
				} `json:"items"`
			} `json:"cpuPartitions"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshaling generated schema: %v", err)
	}

	item := doc.Properties.CPUPartitions.Items
	wantRoles := []string{"reserved", "default", "shared", "exclusive"}
	if !reflect.DeepEqual(item.Properties.Role.Enum, wantRoles) {
		t.Errorf("role enum = %v, want %v", item.Properties.Role.Enum, wantRoles)
	}
	if len(item.Properties.SMT.OneOf) != 2 {
		t.Fatalf("smt oneOf has %d branches, want boolean and integer", len(item.Properties.SMT.OneOf))
	}
	if got := item.Properties.SMT.OneOf[0].Type; got != "boolean" {
		t.Errorf("first smt branch is %q, want boolean", got)
	}
	if got := item.Properties.SMT.OneOf[1].Type; got != "integer" {
		t.Errorf("second smt branch is %q, want integer", got)
	}
	if item.Properties.SMT.OneOf[1].Minimum == nil || *item.Properties.SMT.OneOf[1].Minimum != 1 {
		t.Errorf("smt integer branch minimum = %v, want 1", item.Properties.SMT.OneOf[1].Minimum)
	}
	for _, want := range []string{"name", "role", "cpus"} {
		if !slices.Contains(item.Required, want) {
			t.Errorf("partition item does not require %q, got %v", want, item.Required)
		}
	}
	if slices.Contains(item.Required, "smt") {
		t.Errorf("partition item requires smt, which is optional: %v", item.Required)
	}
}

// TestGenerateDriverConfigSchema_ProfilePartitionConstraints: the partitions of
// a profile are the same partitions, so a file the fleet-wide list would be
// rejected for must be rejected inside a profile too.
func TestGenerateDriverConfigSchema_ProfilePartitionConstraints(t *testing.T) {
	out, err := GenerateDriverConfigSchema()
	if err != nil {
		t.Fatalf("GenerateDriverConfigSchema() error: %v", err)
	}

	type partitionItem struct {
		Properties struct {
			Role struct {
				Enum []string `json:"enum"`
			} `json:"role"`
			SMT struct {
				OneOf []struct {
					Type string `json:"type"`
				} `json:"oneOf"`
			} `json:"smt"`
		} `json:"properties"`
	}
	var doc struct {
		Properties struct {
			CPUPartitions struct {
				Items partitionItem `json:"items"`
			} `json:"cpuPartitions"`
			Profiles struct {
				AdditionalProperties struct {
					Properties struct {
						CPUPartitions struct {
							Items partitionItem `json:"items"`
						} `json:"cpuPartitions"`
					} `json:"properties"`
				} `json:"additionalProperties"`
			} `json:"profiles"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshaling generated schema: %v", err)
	}

	fleetWide := doc.Properties.CPUPartitions.Items
	inProfile := doc.Properties.Profiles.AdditionalProperties.Properties.CPUPartitions.Items
	if !reflect.DeepEqual(fleetWide, inProfile) {
		t.Errorf("a partition inside a profile is constrained as %+v, want the fleet-wide %+v", inProfile, fleetWide)
	}
	if len(inProfile.Properties.Role.Enum) == 0 || len(inProfile.Properties.SMT.OneOf) == 0 {
		t.Errorf("a partition inside a profile carries no role enum or smt branches: %+v", inProfile)
	}
}
