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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var clusterValidateTests = []struct {
	test    string
	cluster KubeCluster
	errs    []string
}{
	{
		test: "valid cluster config",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "id",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "id",
				},
			},
		},
		errs: nil,
	},
	{
		test: "invalid cluster config",
		cluster: KubeCluster{
			Name:      "",
			ClusterId: "id",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "id",
				},
			},
		},
		errs: []string{"config error: All items in the 'clusters' list must have 'name', 'masterURL', 'id' and 'namespaces' keys defined"},
	},
	{
		test: "cluster id too long",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "clusteridtoolong",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "id",
				},
			},
		},
		errs: []string{"`clusters[].id` must be less than 13 characters"},
	},
	{
		test: "cluster id invalid",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "CapsId",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "id",
				},
			},
		},
		errs: []string{"`clusters[].id` can only contain lowercase alphanumeric characters"},
	},
	{
		test: "duplicate namespace ids",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "id",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "id1",
				},
				{
					Name:        "namespace2",
					NamespaceId: "id1",
				},
			},
		},
		errs: []string{"duplicate namespace id found in namespaces configuration: 'id1'"},
	},
	{
		test: "namespace too long",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "id",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "namespaceidtoolong",
				},
			},
		},
		errs: []string{"namespace `id`s must be less than 13 characters"},
	},
	{
		test: "invalid namespace id",
		cluster: KubeCluster{
			Name:      "valid-cluster",
			ClusterId: "id",
			MasterURL: "masterURL",
			Namespaces: []KubeNamespace{
				{
					Name:        "namespace",
					NamespaceId: "CapsId",
				},
			},
		},
		errs: []string{"namespace `id`s can only contain lowercase alphanumeric characters"},
	},
}

func TestClusterValidation(t *testing.T) {
	for _, test := range clusterValidateTests {
		t.Run(test.test, func(t *testing.T) {
			errs := ValidateCluster(test.cluster)
			assert.Equal(t, test.errs, errs, "errors should match")
		})
	}
}
