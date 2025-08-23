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

package repository

import (
	"testing"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/stretchr/testify/assert"
)

type NewRepoTestInput struct {
	clusters         []domain.KubeCluster
	template         string
	sparkManagerPort string
	debugPorts       map[string]config.DebugPort
}

var newRepoTests = []struct {
	test     string
	input    NewRepoTestInput
	expected map[string]string
	err      string
}{
	{
		test: "Happy path, cluster with port no overrides/errors",
		input: NewRepoTestInput{
			clusters: []domain.KubeCluster{
				{
					Name: "testCluster",
				},
			},
			template:         "{{.clusterName}}-endpoint",
			sparkManagerPort: "9090",
		},
		expected: map[string]string{
			"testCluster": "http://testCluster-endpoint:9090",
		},
	},
	{
		test: "Multiple cluster with port no overrides/errors",
		input: NewRepoTestInput{
			clusters: []domain.KubeCluster{
				{
					Name: "testCluster",
				},
				{
					Name: "testCluster2",
				},
			},
			template:         "{{.clusterName}}-endpoint",
			sparkManagerPort: "9090",
		},
		expected: map[string]string{
			"testCluster":  "http://testCluster-endpoint:9090",
			"testCluster2": "http://testCluster2-endpoint:9090",
		},
	},
	{
		test: "Single cluster with port no override",
		input: NewRepoTestInput{
			clusters: []domain.KubeCluster{
				{
					Name: "testCluster",
				},
			},
			template:         "{{.clusterName}}-endpoint",
			sparkManagerPort: "9090",
			debugPorts: map[string]config.DebugPort{
				"testCluster": {
					SparkManagerPort: "9091",
				},
			},
		},
		expected: map[string]string{
			"testCluster": "http://testCluster-endpoint:9091",
		},
	},
	{
		test: "Multiple cluster with port no override",
		input: NewRepoTestInput{
			clusters: []domain.KubeCluster{
				{
					Name: "testCluster",
				},
				{
					Name: "testCluster2",
				},
			},
			template:         "{{.clusterName}}-endpoint",
			sparkManagerPort: "9090",
			debugPorts: map[string]config.DebugPort{
				"testCluster": {
					SparkManagerPort: "9091",
				},
				"testCluster2": {
					SparkManagerPort: "9092",
				},
			},
		},
		expected: map[string]string{
			"testCluster":  "http://testCluster-endpoint:9091",
			"testCluster2": "http://testCluster2-endpoint:9092",
		},
	},
	{
		test: "Bad template",
		input: NewRepoTestInput{
			clusters: []domain.KubeCluster{
				{
					Name: "testCluster",
				},
			},
			template: "{{.clusterName.bad}}-endpoint",
		},
		err: "error while formatting SparkManager Hostname:",
	},
}

func TestNewRepository(t *testing.T) {

	for _, test := range newRepoTests {
		newRepo, err := NewSparkManagerRepository(
			test.input.clusters,
			test.input.template,
			test.input.sparkManagerPort,
			test.input.debugPorts,
		)

		if test.err != "" {
			assert.Contains(t, err.Error(), test.err, "errors should be equal")
			return
		}

		assert.Equal(t, test.expected, newRepo.ClusterEndpoints, "ClusterEndpoints map should match")
	}
}
