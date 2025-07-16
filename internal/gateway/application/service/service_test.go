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

package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/slackhq/spark-gateway/pkg/config"

	"github.com/slackhq/spark-gateway/internal/gateway/cluster"
	"github.com/slackhq/spark-gateway/internal/gateway/router"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/slackhq/spark-gateway/pkg/model"
)

var sgConfig config.SparkGatewayConfig = config.SparkGatewayConfig{
	SelectorKey:   "spark-gateway/owned",
	SelectorValue: "true",
}

var gatewayConfig config.GatewayConfig = config.GatewayConfig{
	StatusUrlTemplates: model.StatusUrlTemplates{
		SparkUI:        "{{.Status.DriverInfo.WebUIIngressAddress}}",
		SparkHistoryUI: "https://spark-history-{{.ObjectMeta.Namespace}}.test.com/history/{{.Status.SparkApplicationID}}/jobs",
		LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22{{.ObjectMeta.Name}}-driver%22')",
	},
}

var testIdGen = model.GatewayIdGenerator{
	UuidGenerator: func() (string, error) {
		return "testid", nil
	},
}

func TestServiceGetNotFound(t *testing.T) {
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s'", name))
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	gatewayApp, err := appService.Get(context.Background(), "clusterid-nsid-testid")

	assert.Equal(t, (*model.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting SparkApplication 'clusterid-nsid-testid'", "error should match")

}
func TestServiceGetNoUserFound(t *testing.T) {

	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
			return &v1beta2.SparkApplication{}, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s':", name))
		}},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	gatewayApp, err := appService.Get(context.Background(), "clusterid-nsid-testid")

	assert.Equal(t, (*model.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting SparkApplication 'clusterid-nsid-testid':", "error should match")

}
func TestServiceGet(t *testing.T) {
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
			return &v1beta2.SparkApplication{
				ObjectMeta: v1.ObjectMeta{
					Name:      "clusterid-nsid-testid",
					Namespace: "namespace",
					Labels:    map[string]string{"spark-gateway/user": "user", "spark-gateway/owned": "true"},
				},
				Status: v1beta2.SparkApplicationStatus{
					SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
					DriverInfo: v1beta2.DriverInfo{
						WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
					},
				}}, nil
		}},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	expected := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-nsid-testid",
				Namespace: "namespace",
				Labels:    map[string]string{"spark-gateway/user": "user", "spark-gateway/owned": "true"},
			},
			Spec: v1beta2.SparkApplicationSpec{},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
				DriverInfo: v1beta2.DriverInfo{
					WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
				},
			},
		},
		GatewayId: "clusterid-nsid-testid",
		Cluster:   "cluster",
		User:      "user",
		SparkLogURLs: model.SparkLogURLs{
			SparkUI:        "http://spark-ui-dev.test.com/sparkApp",
			LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22clusterid-nsid-testid-driver%22')",
			SparkHistoryUI: "https://spark-history-namespace.test.com/history/spark-64edcd63474d4ed93fec4766471eedad/jobs",
		},
	}

	gatewayApp, _ := appService.Get(context.Background(), "clusterid-nsid-testid")

	assert.Equal(t, expected, gatewayApp, "returned GatewayApplication should match")

}

func TestServiceCreateNoLabels(t *testing.T) {
	username := "user"
	localApps := map[string]*v1beta2.SparkApplication{}
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			CreateFunc: func(ctx context.Context, cluster model.KubeCluster, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
				application.Status = v1beta2.SparkApplicationStatus{
					SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
					DriverInfo: v1beta2.DriverInfo{
						WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
					},
				}
				localApps[application.Name] = application
				return localApps[application.Name], nil
			},
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return localApps[name], nil
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		gatewayIdGenerator: testIdGen,
		config:             gatewayConfig,
		selectorKey:        sgConfig.SelectorKey,
		selectorValue:      sgConfig.SelectorValue,
	}

	inApp := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "namespace",
		},
	}

	expected := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:        "clusterid-nsid-testid",
				Namespace:   "namespace",
				Labels:      map[string]string{"spark-gateway/user": username, "spark-gateway/owned": "true"},
				Annotations: map[string]string{},
			},
			Spec: v1beta2.SparkApplicationSpec{
				ProxyUser: &username,
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
				DriverInfo: v1beta2.DriverInfo{
					WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
				},
			},
		},
		GatewayId: "clusterid-nsid-testid",
		Cluster:   "cluster",
		User:      username,
		SparkLogURLs: model.SparkLogURLs{
			SparkUI:        "http://spark-ui-dev.test.com/sparkApp",
			LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22clusterid-nsid-testid-driver%22')",
			SparkHistoryUI: "https://spark-history-namespace.test.com/history/spark-64edcd63474d4ed93fec4766471eedad/jobs",
		},
	}

	gatewayApp, err := appService.Create(context.Background(), inApp, username)

	assert.Equal(t, expected, gatewayApp, "returned GatewayApplication should match")
	assert.Equal(t, nil, err, "err should be nil")
}
func TestServiceCreateLabelsExist(t *testing.T) {
	username := "user"
	localApps := map[string]*v1beta2.SparkApplication{}
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			CreateFunc: func(ctx context.Context, cluster model.KubeCluster, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
				application.Status = v1beta2.SparkApplicationStatus{
					SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
					DriverInfo: v1beta2.DriverInfo{
						WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
					},
				}
				localApps[application.Name] = application
				return localApps[application.Name], nil
			},
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return localApps[name], nil
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		gatewayIdGenerator: testIdGen,
		config:             gatewayConfig,
		selectorKey:        sgConfig.SelectorKey,
		selectorValue:      sgConfig.SelectorValue,
	}

	inApp := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "originalName",
			Namespace: "namespace",
			Labels:    map[string]string{"test": "label"},
		},
	}

	expected := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-nsid-testid",
				Namespace: "namespace",
				Labels: map[string]string{
					"spark-gateway/user":  username,
					"spark-gateway/owned": "true",
					"test":                "label",
				},
				Annotations: map[string]string{"applicationName": "originalName"},
			},
			Spec: v1beta2.SparkApplicationSpec{
				ProxyUser: &username,
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
				DriverInfo: v1beta2.DriverInfo{
					WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
				},
			},
		},
		GatewayId: "clusterid-nsid-testid",
		Cluster:   "cluster",
		User:      username,
		SparkLogURLs: model.SparkLogURLs{
			SparkUI:        "http://spark-ui-dev.test.com/sparkApp",
			LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22clusterid-nsid-testid-driver%22')",
			SparkHistoryUI: "https://spark-history-namespace.test.com/history/spark-64edcd63474d4ed93fec4766471eedad/jobs",
		},
	}

	gatewayApp, err := appService.Create(context.Background(), inApp, username)

	assert.Equal(t, expected, gatewayApp, "returned GatewayApplication should match")
	assert.Equal(t, nil, err, "err should be nil")
}
func TestServiceCreateNoName(t *testing.T) {
	username := "user"
	localApps := map[string]*v1beta2.SparkApplication{}
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			CreateFunc: func(ctx context.Context, cluster model.KubeCluster, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
				application.Status = v1beta2.SparkApplicationStatus{
					SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
					DriverInfo: v1beta2.DriverInfo{
						WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
					},
				}
				localApps[application.Name] = application
				return localApps[application.Name], nil
			},
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return localApps[name], nil
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		gatewayIdGenerator: testIdGen,
		config:             gatewayConfig,
		selectorKey:        sgConfig.SelectorKey,
		selectorValue:      sgConfig.SelectorValue,
	}

	inApp := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "namespace",
			Labels:    map[string]string{"test": "label"},
		},
	}

	expected := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-nsid-testid",
				Namespace: "namespace",
				Labels: map[string]string{
					"spark-gateway/user":  username,
					"spark-gateway/owned": "true",
					"test":                "label",
				},
				Annotations: map[string]string{},
			},
			Spec: v1beta2.SparkApplicationSpec{
				ProxyUser: &username,
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-64edcd63474d4ed93fec4766471eedad",
				DriverInfo: v1beta2.DriverInfo{
					WebUIIngressAddress: "http://spark-ui-dev.test.com/sparkApp",
				},
			},
		},
		GatewayId: "clusterid-nsid-testid",
		Cluster:   "cluster",
		User:      username,
		SparkLogURLs: model.SparkLogURLs{
			SparkUI:        "http://spark-ui-dev.test.com/sparkApp",
			LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22clusterid-nsid-testid-driver%22')",
			SparkHistoryUI: "https://spark-history-namespace.test.com/history/spark-64edcd63474d4ed93fec4766471eedad/jobs",
		},
	}

	gatewayApp, err := appService.Create(context.Background(), inApp, username)
	assert.Nil(t, err, "Despite DB failure, method should not throw an error since the sparkapp was submitted")
	assert.Equal(t, expected, gatewayApp, "returned GatewayApplication should match")
	assert.Equal(t, nil, err, "err should be nil")
}

func TestServiceCreateError(t *testing.T) {

	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			CreateFunc: func(ctx context.Context, cluster model.KubeCluster, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
				return nil, errors.New("test error")
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		gatewayIdGenerator: testIdGen,
		config:             gatewayConfig,
	}

	inApp := &v1beta2.SparkApplication{ObjectMeta: v1.ObjectMeta{Namespace: "namespace", Name: "sparkApp"}}

	gatewayApp, err := appService.Create(context.Background(), inApp, "")

	assert.Equal(t, (*model.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error creating SparkApplication 'namespace/clusterid-nsid-testid': test error", "err should match")
}

func TestServiceCreateNoNamespace_SparkApplicationValidator(t *testing.T) {
	username := "user"
	appService := service{
		sparkAppRepo:       &SparkApplicationRepositoryMock{},
		clusterRepository:  &cluster.ClusterRepositoryMock{},
		clusterRouter:      &router.ClusterRouterMock{},
		gatewayIdGenerator: testIdGen,
		config:             gatewayConfig,
		selectorKey:        sgConfig.SelectorKey,
		selectorValue:      sgConfig.SelectorValue,
	}

	inApp := &v1beta2.SparkApplication{}

	gatewayApp, err := appService.Create(context.Background(), inApp, username)

	assert.Equal(t, (*model.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	if assert.Error(t, err, "Should throw error due to missing namespace in SparkApplication") {
		assert.Equal(t, "submitted SparkApplication is invalid: [namespace should not be empty]", err.Error())
	}
}

func TestServiceStatusError(t *testing.T) {
	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return nil, errors.New("test error")
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	_, err := appService.Status(context.Background(), "clusterid-nsid-testid")

	assert.Contains(t, err.Error(), "error getting status for SparkApplication 'clusterid-nsid-testid': test error", "err should match")
}
func TestServiceStatus(t *testing.T) {

	retApp := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "clusterid-nsid-testid",
			Namespace: "test",
			Labels: map[string]string{
				"spark-gateway/user":  "user",
				"spark-gateway/owned": "true",
			},
		},
		Spec:   v1beta2.SparkApplicationSpec{},
		Status: v1beta2.SparkApplicationStatus{SparkApplicationID: "appId"},
	}

	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			GetFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) (*v1beta2.SparkApplication, error) {
				return retApp, nil
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	expected := &v1beta2.SparkApplicationStatus{SparkApplicationID: "appId"}

	resp, _ := appService.Status(context.Background(), "clusterid-nsid-testid")

	assert.Equal(t, expected, resp, "returned response should match")
}

func TestServiceDeleteError(t *testing.T) {

	appService := service{
		sparkAppRepo: &SparkApplicationRepositoryMock{
			DeleteFunc: func(ctx context.Context, cluster model.KubeCluster, namespace string, name string) error {
				return errors.New("test error")
			},
		},
		clusterRepository: &cluster.ClusterRepositoryMock{
			GetByIdFunc: func(clusterId string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		clusterRouter: &router.ClusterRouterMock{
			GetClusterFunc: func(ctx context.Context, namespace string) (*model.KubeCluster, error) {
				return &model.KubeCluster{
					Name:      "cluster",
					ClusterId: "clusterid",
					Namespaces: []model.KubeNamespace{
						{
							Name:        "namespace",
							NamespaceId: "nsid",
						},
					},
				}, nil
			},
		},
		config: gatewayConfig,
	}

	assert.Contains(t, appService.Delete(context.Background(), "clusterid-nsid-testid").Error(), "error deleting SparkApplication 'clusterid-nsid-testid': test error", "errors should match")

}
