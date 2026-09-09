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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrontierInputDigestLength(t *testing.T) {
	digest := FrontierInputDigest([]string{"uid-1", "uid-2", "uid-3"})
	require.Len(t, digest, 64)
	assert.LessOrEqual(t, len(digest), DeviceAttributeMaxValueLength)
}

func TestFrontierInputDigestSortedStability(t *testing.T) {
	a := FrontierInputDigest([]string{"b", "a", "c"})
	b := FrontierInputDigest([]string{"c", "a", "b"})
	assert.Equal(t, a, b)
}

func TestFrontierInputDigestDiffersOnDifferentUIDs(t *testing.T) {
	a := FrontierInputDigest([]string{"uid-1"})
	b := FrontierInputDigest([]string{"uid-2"})
	assert.NotEqual(t, a, b)
}

func TestFrontierInputDigestEmpty(t *testing.T) {
	digest := FrontierInputDigest([]string{})
	require.Len(t, digest, 64)
}

type claimUID string

func TestFrontierInputDigestGenericType(t *testing.T) {
	a := FrontierInputDigest([]claimUID{"uid-1", "uid-2"})
	b := FrontierInputDigest([]string{"uid-1", "uid-2"})
	assert.Equal(t, a, b)
}
