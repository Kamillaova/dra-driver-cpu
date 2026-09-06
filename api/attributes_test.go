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

package api

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAttributeNamesAreValidQualifiedNames pins every name against what the API
// server accepts, because an invalid one is rejected on every ResourceSlice
// write the node makes rather than on the one device that carries it: the domain
// is ours, the segment after the slash must be a C identifier, and it may not
// exceed DeviceMaxIDLength.
func TestAttributeNamesAreValidQualifiedNames(t *testing.T) {
	const domain = "dra.cpu"
	const maxSegment = 32

	names := []string{
		AttributeSocketID,
		AttributeSMTEnabled,
		AttributeCacheL3ID,
		AttributeCoreType,
		AttributeCoreID,
		AttributeCPUID,
		AttributeNumCPUs,
		AttributeThreadsPerCore,
		AttributeLargestUncoreCacheCPUs,
		AttributeUncoreCachesInGroup,
		AttributePartition,
		AttributeRole,
		AttributeAllocatedNumCPUs,
		AttributeCPUSet,
		AttributeRelocatable,
		AttributeAlignment,
		AttributeRepairRounds,
		AttributeFrontierInput,
	}

	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			gotDomain, segment, found := strings.Cut(name, "/")
			require.True(t, found, "name carries no domain")
			assert.Equal(t, domain, gotDomain)
			assert.LessOrEqual(t, len(segment), maxSegment)
			assert.True(t, isCIdentifier(segment), "%q is not a C identifier", segment)

			_, duplicate := seen[name]
			assert.False(t, duplicate, "name is declared twice")
			seen[name] = struct{}{}
		})
	}
}

func isCIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// TestRepairRoundsUnreachableIsNotACount: a reader splits the attribute on
// commas and has to tell a round count from "no repair found", so the marker
// must not parse as a number.
func TestRepairRoundsUnreachableIsNotACount(t *testing.T) {
	_, err := strconv.Atoi(RepairRoundsUnreachable)
	assert.Error(t, err)
}
