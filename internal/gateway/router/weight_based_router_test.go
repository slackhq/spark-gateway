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

package router

import (
	"testing"

	"github.com/slackhq/spark-gateway/pkg/util"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"

	cfgPkg "github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/model"
)

func TestGenerateMap(t *testing.T) {
	tests := []struct {
		name     string
		labels   []*io_prometheus_client.LabelPair
		expected map[string]string
	}{
		{
			name: "Test Generate Map from list of label pairs",
			labels: []*io_prometheus_client.LabelPair{
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("cluster"),
					Value: util.Ptr("ctra"),
				}),
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("namespace"),
					Value: util.Ptr("ns1"),
				}),
			},
			expected: map[string]string{
				"cluster":   "ctra",
				"namespace": "ns1",
			},
		},
	}

	for _, test := range tests {
		m := GenerateMap(test.labels)
		assert.Equal(t, test.expected, m, "failed: "+test.name)
	}
}

func TestContainsLabels(t *testing.T) {
	tests := []struct {
		name         string
		labels       []*io_prometheus_client.LabelPair
		targetLabels map[string]string
		expected     bool
	}{
		{
			name: "Check all label keys",
			labels: []*io_prometheus_client.LabelPair{
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("cluster"),
					Value: util.Ptr("ctra"),
				}),
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("namespace"),
					Value: util.Ptr("ns1"),
				}),
			},
			targetLabels: map[string]string{
				"cluster":   "ctra",
				"namespace": "ns1",
			},
			expected: true,
		},
		{
			name: "TargetLabel doesnt not exist",
			labels: []*io_prometheus_client.LabelPair{
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("cluster"),
					Value: util.Ptr("ctrb"),
				}),
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("namespace"),
					Value: util.Ptr("ns1"),
				}),
			},
			targetLabels: map[string]string{
				"cluster":   "ctra",
				"namespace": "ns1",
			},
			expected: false,
		},
		{
			name: "Extra labels in Labelpair list ignored",
			labels: []*io_prometheus_client.LabelPair{
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("cluster"),
					Value: util.Ptr("ctra"),
				}),
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("namespace"),
					Value: util.Ptr("ns1"),
				}),
				util.Ptr(io_prometheus_client.LabelPair{
					Name:  util.Ptr("testKey"),
					Value: util.Ptr("testValue"),
				}),
			},
			targetLabels: map[string]string{
				"cluster":   "ctra",
				"namespace": "ns1",
			},
			expected: true,
		},
	}
	for _, test := range tests {
		m := ContainsLabels(test.labels, test.targetLabels)
		assert.Equal(t, test.expected, m, "failed test: %s", test.name)
	}
}

func TestGetTargetMetrics(t *testing.T) {
	tests := []struct {
		name         string
		metrics      []*io_prometheus_client.Metric
		targetLabels map[string]string
		expected     []*io_prometheus_client.Metric
	}{
		{
			name: "Get Metric with empty namespace label",
			metrics: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr("ns1"),
						}),
					},
				},
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr(""),
						}),
					},
				},
			},
			targetLabels: map[string]string{
				"cluster":   "ctra",
				"namespace": "",
			},
			expected: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr(""),
						}),
					},
				},
			},
		},
		{
			name: "Get Metric with ns1 namespace label",
			metrics: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr("ns1"),
						}),
					},
				},
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr(""),
						}),
					},
				},
			},
			targetLabels: map[string]string{
				"cluster":   "ctra",
				"namespace": "ns1",
			},
			expected: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr("ns1"),
						}),
					},
				},
			},
		},
		{
			name: "No matching metric labels",
			metrics: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr("ns1"),
						}),
					},
				},
				{
					Label: []*io_prometheus_client.LabelPair{
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("cluster"),
							Value: util.Ptr("ctra"),
						}),
						util.Ptr(io_prometheus_client.LabelPair{
							Name:  util.Ptr("namespace"),
							Value: util.Ptr(""),
						}),
					},
				},
			},
			targetLabels: map[string]string{
				"cluster":   "ctrb",
				"namespace": "ns1",
			},
			expected: []*io_prometheus_client.Metric(nil),
		},
	}
	for _, test := range tests {
		m := GetTargetMetrics(test.metrics, test.targetLabels)
		assert.Equal(t, test.expected, m, "failed test: %s", test.name)
	}
}

func TestGetTargetLabels(t *testing.T) {
	tests := []struct {
		name                string
		clusterRouterConfig cfgPkg.ClusterRouter
		clusterName         string
		namespace           string
		expected            map[string]string
	}{
		{
			name: "Create target labels with empty namespace label",
			clusterRouterConfig: cfgPkg.ClusterRouter{
				Dimension: "cluster",
				PrometheusQuery: cfgPkg.PrometheusQuery{
					AdditionalLabels: map[string]string{
						"specialKey": "specialValue",
					},
				},
			},
			clusterName: "ctra",
			namespace:   "ns1",
			expected: map[string]string{
				"cluster":    "ctra",
				"namespace":  "",
				"specialKey": "specialValue",
			},
		},
		{
			name: "Create target labels with ns1 namespace label",
			clusterRouterConfig: cfgPkg.ClusterRouter{
				Dimension: "namespace",
				PrometheusQuery: cfgPkg.PrometheusQuery{
					AdditionalLabels: map[string]string{
						"specialKey": "specialValue",
					},
				},
			},
			clusterName: "ctra",
			namespace:   "ns1",
			expected: map[string]string{
				"cluster":    "ctra",
				"namespace":  "ns1",
				"specialKey": "specialValue",
			},
		},
	}
	for _, test := range tests {
		m := GetTargetLabels(test.clusterRouterConfig, test.clusterName, test.namespace)
		assert.Equal(t, test.expected, m, "failed test: %s", test.name)
	}
}

func TestReadWeightConfigs(t *testing.T) {
	tests := []struct {
		name                string
		metricsMap          map[string]*metric
		clusters            []model.KubeCluster
		routerConfig        cfgPkg.ClusterRouter
		namespace           string
		expectedMetricsMap  map[string]*metric
		expectedTotalWeight *float64
		expectedError       error
	}{
		{
			name:       "cluster dimension",
			metricsMap: make(map[string]*metric),
			clusters: []model.KubeCluster{
				model.KubeCluster{
					Name:          "ctra",
					ClusterId:     "a",
					RoutingWeight: float64(10),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns1",
							RoutingWeight: float64(8),
						},
					},
				},
			},
			routerConfig: cfgPkg.ClusterRouter{
				Dimension: "cluster",
			},
			namespace: "ns1",
			expectedMetricsMap: map[string]*metric{
				"a": &metric{
					Weight:      float64(10),
					WeightRatio: float64(1),
				},
			},
			expectedTotalWeight: util.Ptr(float64(10)),
		},
		{
			name:       "namespace dimension",
			metricsMap: make(map[string]*metric),
			clusters: []model.KubeCluster{
				model.KubeCluster{
					Name:          "ctra",
					ClusterId:     "a",
					RoutingWeight: float64(10),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(5),
						},
					},
				},
			},
			routerConfig: cfgPkg.ClusterRouter{
				Dimension: "namespace",
			},
			namespace: "ns2",
			expectedMetricsMap: map[string]*metric{
				"a": &metric{
					Weight:      float64(5),
					WeightRatio: float64(1),
				},
			},
			expectedTotalWeight: util.Ptr(float64(5)),
		},
		{
			name:       "cluster dimension, multiple clusters",
			metricsMap: make(map[string]*metric),
			clusters: []model.KubeCluster{
				model.KubeCluster{
					Name:          "ctra",
					ClusterId:     "a",
					RoutingWeight: float64(5),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(5),
						},
					},
				},
				model.KubeCluster{
					Name:          "ctrb",
					ClusterId:     "b",
					RoutingWeight: float64(20),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(5),
						},
					},
				},
			},
			routerConfig: cfgPkg.ClusterRouter{
				Dimension: "cluster",
			},
			namespace: "ns2",
			expectedMetricsMap: map[string]*metric{
				"a": &metric{
					Weight:      float64(5),
					WeightRatio: float64(0.2),
				},
				"b": &metric{
					Weight:      float64(20),
					WeightRatio: float64(0.8),
				},
			},
			expectedTotalWeight: util.Ptr(float64(25)),
		},
		{
			name:       "namespace dimension, multiple clusters",
			metricsMap: make(map[string]*metric),
			clusters: []model.KubeCluster{
				model.KubeCluster{
					Name:          "ctra",
					ClusterId:     "a",
					RoutingWeight: float64(5),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(15),
						},
					},
				},
				model.KubeCluster{
					Name:          "ctrb",
					ClusterId:     "b",
					RoutingWeight: float64(20),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(5),
						},
					},
				},
				model.KubeCluster{
					Name:          "ctrc",
					ClusterId:     "c",
					RoutingWeight: float64(10),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns1",
							RoutingWeight: float64(5),
						},
					},
				},
			},
			routerConfig: cfgPkg.ClusterRouter{
				Dimension: "namespace",
			},
			namespace: "ns2",
			expectedMetricsMap: map[string]*metric{
				"a": &metric{
					Weight:      float64(15),
					WeightRatio: float64(0.75),
				},
				"b": &metric{
					Weight:      float64(5),
					WeightRatio: float64(0.25),
				},
			},
			expectedTotalWeight: util.Ptr(float64(20)),
		},
		{
			name:       "namespace dimension, namespace doesn't exist in ctra so only ctrb in metricsMap",
			metricsMap: make(map[string]*metric),
			clusters: []model.KubeCluster{
				model.KubeCluster{
					Name:          "ctra",
					ClusterId:     "a",
					RoutingWeight: float64(10),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "ns2",
							RoutingWeight: float64(5),
						},
					},
				},
				model.KubeCluster{
					Name:          "ctrb",
					ClusterId:     "b",
					RoutingWeight: float64(10),
					Namespaces: []model.KubeNamespace{
						model.KubeNamespace{
							Name:          "special-ns",
							RoutingWeight: float64(5),
						},
					},
				},
			},
			routerConfig: cfgPkg.ClusterRouter{
				Dimension: "namespace",
			},
			namespace: "special-ns",
			expectedMetricsMap: map[string]*metric{
				"b": &metric{
					Weight:      float64(5),
					WeightRatio: float64(1),
				},
			},
			expectedTotalWeight: util.Ptr(float64(5)),
		},
	}
	for _, test := range tests {
		m, w := ReadWeightConfigs(test.metricsMap, test.clusters, test.routerConfig, test.namespace)
		assert.Equal(t, test.expectedMetricsMap, m, "failed test: %s", test.name)
		assert.Equal(t, test.expectedTotalWeight, w, "failed test: %s", test.name)
	}
}

func TestWeightedChooseClusterID(t *testing.T) {
	tests := []struct {
		name        string
		metricsMap  map[string]*metric
		totalMetric float64
		expected    string
	}{
		{
			name: "cluster b is chosen due to its smaller metric value",
			metricsMap: map[string]*metric{
				"a": &metric{
					Metric:      float64(100),
					WeightRatio: float64(0.5),
				},
				"b": &metric{
					Metric:      float64(10),
					WeightRatio: float64(0.5),
				},
			},
			totalMetric: 110,
			expected:    "b",
		},
		{
			name: "total metric value is 0, cluster with highest weight is chosen",
			metricsMap: map[string]*metric{
				"a": &metric{
					Metric:      float64(0),
					WeightRatio: float64(0.2),
				},
				"b": &metric{
					Metric:      float64(0),
					WeightRatio: float64(0.8),
				},
			},
			totalMetric: 0,
			expected:    "b",
		},
		{
			name: "total metric value is 0 and both clusters have same weight, first cluster is chosen",
			metricsMap: map[string]*metric{
				"a": &metric{
					Metric:      float64(0),
					WeightRatio: float64(0.5),
				},
				"b": &metric{
					Metric:      float64(0),
					WeightRatio: float64(0.5),
				},
			},
			totalMetric: 0,
			expected:    "a",
		},
		{
			name: "test different metrics and ratios",
			metricsMap: map[string]*metric{
				"a": &metric{
					Metric:      float64(0),
					WeightRatio: float64(0.1),
				},
				"b": &metric{
					Metric:      float64(3),
					WeightRatio: float64(0.9),
				},
			},
			totalMetric: 3,
			expected:    "a",
		},
	}
	for _, test := range tests {
		c := chooseClusterID(test.metricsMap, test.totalMetric)
		assert.Equal(t, test.expected, c, "failed test: %s", test.name)
	}
}
