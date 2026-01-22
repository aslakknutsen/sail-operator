// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package install

import (
	"os"
	"testing"

	v1 "github.com/istio-ecosystem/sail-operator/api/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/config"
	"github.com/istio-ecosystem/sail-operator/pkg/istioversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"istio.io/istio/pkg/ptr"
)

func TestLoadPresets(t *testing.T) {
	presets, err := loadPresets()
	require.NoError(t, err)

	// Verify all expected presets are loaded
	assert.Contains(t, presets, PresetGatewayAPI)
	assert.Contains(t, presets, PresetGatewayAPIAmbient)
	assert.Contains(t, presets, PresetFull)

	// Verify gateway-api preset
	gatewayAPI := presets[PresetGatewayAPI]
	assert.Equal(t, PresetGatewayAPI, gatewayAPI.Name)
	assert.Equal(t, "default", gatewayAPI.BaseProfile)
	assert.True(t, gatewayAPI.Components.Istiod)
	assert.False(t, gatewayAPI.Components.CNI)
	assert.False(t, gatewayAPI.Components.ZTunnel)

	// Verify gateway-api-ambient preset
	gatewayAPIAmbient := presets[PresetGatewayAPIAmbient]
	assert.Equal(t, PresetGatewayAPIAmbient, gatewayAPIAmbient.Name)
	assert.Equal(t, "openshift-ambient", gatewayAPIAmbient.BaseProfile)
	assert.True(t, gatewayAPIAmbient.Components.Istiod)
	assert.True(t, gatewayAPIAmbient.Components.CNI)
	assert.True(t, gatewayAPIAmbient.Components.ZTunnel)

	// Verify full preset
	full := presets[PresetFull]
	assert.Equal(t, PresetFull, full.Name)
	assert.Equal(t, "openshift", full.BaseProfile)
	assert.True(t, full.Components.Istiod)
	assert.True(t, full.Components.CNI)
	assert.False(t, full.Components.ZTunnel)
}

func TestGetAvailablePresetNames(t *testing.T) {
	names := GetAvailablePresetNames()

	assert.Len(t, names, 3)
	assert.Contains(t, names, PresetGatewayAPI)
	assert.Contains(t, names, PresetGatewayAPIAmbient)
	assert.Contains(t, names, PresetFull)
}

func TestOptionsApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		opts     Options
		expected Options
	}{
		{
			name: "empty options gets defaults",
			opts: Options{},
			expected: Options{
				HelmDriver:        "secret",
				DefaultProfile:    "default",
				OperatorNamespace: "istio-system",
			},
		},
		{
			name: "OpenShift platform gets openshift profile",
			opts: Options{
				Platform: config.PlatformOpenShift,
			},
			expected: Options{
				Platform:          config.PlatformOpenShift,
				HelmDriver:        "secret",
				DefaultProfile:    "openshift",
				OperatorNamespace: "istio-system",
			},
		},
		{
			name: "custom values are preserved",
			opts: Options{
				HelmDriver:        "memory",
				DefaultProfile:    "custom",
				OperatorNamespace: "custom-ns",
			},
			expected: Options{
				HelmDriver:        "memory",
				DefaultProfile:    "custom",
				OperatorNamespace: "custom-ns",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.applyDefaults()
			assert.Equal(t, tt.expected.HelmDriver, tt.opts.HelmDriver)
			assert.Equal(t, tt.expected.DefaultProfile, tt.opts.DefaultProfile)
			assert.Equal(t, tt.expected.OperatorNamespace, tt.opts.OperatorNamespace)
		})
	}
}

func TestOverridesApplyDefaults(t *testing.T) {
	tests := []struct {
		name      string
		overrides Overrides
		expected  Overrides
	}{
		{
			name:      "empty overrides gets default namespace",
			overrides: Overrides{},
			expected: Overrides{
				Namespace: "istio-system",
			},
		},
		{
			name: "custom namespace is preserved",
			overrides: Overrides{
				Namespace: "custom-ns",
			},
			expected: Overrides{
				Namespace: "custom-ns",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.overrides.applyDefaults()
			assert.Equal(t, tt.expected.Namespace, tt.overrides.Namespace)
		})
	}
}

func TestMergeValues(t *testing.T) {
	tests := []struct {
		name     string
		base     *v1.Values
		override *v1.Values
		check    func(t *testing.T, result *v1.Values)
	}{
		{
			name:     "nil base and override returns empty Values",
			base:     nil,
			override: nil,
			check: func(t *testing.T, result *v1.Values) {
				assert.NotNil(t, result)
			},
		},
		{
			name: "nil base returns copy of override",
			base: nil,
			override: &v1.Values{
				Pilot: &v1.PilotConfig{
					ReplicaCount: ptr.Of[uint32](2),
				},
			},
			check: func(t *testing.T, result *v1.Values) {
				assert.NotNil(t, result.Pilot)
				assert.Equal(t, uint32(2), *result.Pilot.ReplicaCount)
			},
		},
		{
			name: "nil override returns copy of base",
			base: &v1.Values{
				Pilot: &v1.PilotConfig{
					ReplicaCount: ptr.Of[uint32](3),
				},
			},
			override: nil,
			check: func(t *testing.T, result *v1.Values) {
				assert.NotNil(t, result.Pilot)
				assert.Equal(t, uint32(3), *result.Pilot.ReplicaCount)
			},
		},
		{
			name: "override takes precedence",
			base: &v1.Values{
				Pilot: &v1.PilotConfig{
					ReplicaCount: ptr.Of[uint32](1),
				},
			},
			override: &v1.Values{
				Pilot: &v1.PilotConfig{
					ReplicaCount: ptr.Of[uint32](5),
				},
			},
			check: func(t *testing.T, result *v1.Values) {
				assert.NotNil(t, result.Pilot)
				assert.Equal(t, uint32(5), *result.Pilot.ReplicaCount)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeValues(tt.base, tt.override)
			tt.check(t, result)
		})
	}
}

func TestGetSupportedVersions(t *testing.T) {
	versions := GetSupportedVersions()
	assert.NotEmpty(t, versions)

	// All versions should have a name
	for _, v := range versions {
		assert.NotEmpty(t, v.Name)
	}
}

func TestGetDefaultVersion(t *testing.T) {
	defaultVersion := GetDefaultVersion()
	assert.NotEmpty(t, defaultVersion)

	// Default version should be in the supported versions list
	found := false
	for _, v := range istioversion.List {
		if v.Name == defaultVersion {
			found = true
			break
		}
	}
	assert.True(t, found, "default version should be in supported versions list")
}

func TestNewInstallerValidation(t *testing.T) {
	t.Run("nil KubeConfig", func(t *testing.T) {
		_, err := NewInstaller(Options{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "KubeConfig is required")
	})

	t.Run("nil ResourceFS", func(t *testing.T) {
		// Using a minimal valid rest.Config
		cfg := &rest.Config{Host: "https://localhost:6443"}
		_, err := NewInstaller(Options{
			KubeConfig: cfg,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ResourceFS is required")
	})
}

func TestPresetDescriptions(t *testing.T) {
	presets, err := loadPresets()
	require.NoError(t, err)

	// All presets should have non-empty descriptions
	for name, preset := range presets {
		assert.NotEmpty(t, preset.Description, "preset %s should have a description", name)
	}
}

func TestPresetBaseProfiles(t *testing.T) {
	presets, err := loadPresets()
	require.NoError(t, err)

	// All presets should have valid base profiles
	validProfiles := map[string]bool{
		"openshift":         true,
		"openshift-ambient": true,
		"default":           true,
		"ambient":           true,
	}

	for name, preset := range presets {
		assert.True(t, validProfiles[preset.BaseProfile],
			"preset %s has invalid base profile: %s", name, preset.BaseProfile)
	}
}

func TestFromDirectory(t *testing.T) {
	// Create a temp directory to test FromDirectory
	tempDir := t.TempDir()

	// Create a test file
	testFile := "test.txt"
	testContent := []byte("hello world")
	err := os.WriteFile(tempDir+"/"+testFile, testContent, 0644)
	require.NoError(t, err)

	// Use FromDirectory to create an fs.FS
	dirFS := FromDirectory(tempDir)
	require.NotNil(t, dirFS)

	// Verify we can read from it
	content, err := os.ReadFile(tempDir + "/" + testFile)
	require.NoError(t, err)
	assert.Equal(t, testContent, content)
}
