// Copyright (c) 2025, Salesforce, Inc.
// SPDX-License-Identifier: Apache-2
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

package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
)

type testConfig struct {
	Name string `koanf:"name"`
	Port int    `koanf:"port"`
}

func (t *testConfig) Key() string {
	return ""
}

func (b *testConfig) Unmarshal(k *koanf.Koanf) error {
	if err := k.Unmarshal(b.Key(), b); err != nil {
		return fmt.Errorf("error unmarshaling config [%s]: %w", b.Key(), err)
	}

	return nil
}

func TestConfigUnmarshal_ValidConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config.yaml")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configYaml := `name: "Test"
port: 8080
`

	if _, err := tmpFile.Write([]byte(configYaml)); err != nil {
		t.Fatalf("unable to write to temp file: %v", err)
	}
	tmpFile.Close()

	var conf testConfig
	err = ConfigUnmarshal(tmpFile.Name(), &conf)

	assert.Nil(t, err, "expected no error")
	assert.Equal(t, "Test", conf.Name, "expected name to be Test")
	assert.Equal(t, 8080, conf.Port, "expected port to be 8080")
}

func TestConfigUnmarshal_InvalidConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config.yaml")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configYaml := `name "Test"
port: 8080
`

	if _, err := tmpFile.Write([]byte(configYaml)); err != nil {
		t.Fatalf("unable to write to temp file: %v", err)
	}
	tmpFile.Close()

	var conf testConfig
	err = ConfigUnmarshal(tmpFile.Name(), &conf)

	assert.Contains(t, err.Error(), "error parsing config file:", "expected error")
}
