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

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/repository"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// "github.com/slackhq/spark-gateway/internal/gateway/clusterrouter"
	// "github.com/slackhq/spark-gateway/internal/gateway/repository"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
)

var TEST_USER string = "user"

// Structs
var testSgConfig config.SparkGatewayConfig = config.SparkGatewayConfig{
	SelectorKey:   "spark-gateway/owned",
	SelectorValue: "true",
}

var testGatewayConfig config.GatewayConfig = config.GatewayConfig{
	StatusUrlTemplates: domain.StatusUrlTemplates{
		SparkUITemplate:        "{{.Status.DriverInfo.WebUIIngressAddress}}",
		SparkHistoryUITemplate: "https://spark-history-{{.Namespace}}.test.com/history/{{.Status.SparkApplicationID}}/jobs",
		LogsUITemplate:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22{{.Name}}-driver%22')",
	},
}

var testCluster domain.KubeCluster = domain.KubeCluster{
	Name:      "test-cluster",
	MasterURL: "masterUrl",
	ClusterId: "id",
	Namespaces: []domain.KubeNamespace{
		{
			Name:        "testNamespace",
			NamespaceId: "nsid",
		},
	},
}

var inputSparkApp *v1beta2.SparkApplication = &v1beta2.SparkApplication{
	TypeMeta: v1.TypeMeta{
		Kind:       "SparkApplication",
		APIVersion: "sparkoperator.k8s.io/v1beta2",
	},
	ObjectMeta: v1.ObjectMeta{
		Name:      "appName",
		Namespace: "testNamespace",
		Labels: map[string]string{
			domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
			domain.GATEWAY_USER_LABEL:    "user",
		},
		Annotations: map[string]string{
			domain.GATEWAY_APPLICATION_NAME_ANNOTATION: "appName",
		},
	},
	Spec: v1beta2.SparkApplicationSpec{},
	Status: v1beta2.SparkApplicationStatus{
		SubmissionID:       "test123",
		SparkApplicationID: "sparkAppID",
	},
}

var expectedSparkApp *v1beta2.SparkApplication = &v1beta2.SparkApplication{
	TypeMeta: v1.TypeMeta{
		Kind:       "SparkApplication",
		APIVersion: "sparkoperator.k8s.io/v1beta2",
	},
	ObjectMeta: v1.ObjectMeta{
		Name:      "clusterid-nsid-uuid",
		Namespace: "testNamespace",
		Labels: map[string]string{
			domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
			domain.GATEWAY_USER_LABEL:    "user",
		},
		Annotations: map[string]string{
			domain.GATEWAY_APPLICATION_NAME_ANNOTATION: "appName",
		},
	},
	Spec: v1beta2.SparkApplicationSpec{
		ProxyUser: &TEST_USER,
	},
	Status: v1beta2.SparkApplicationStatus{
		SubmissionID:       "test123",
		SparkApplicationID: "sparkAppID",
	},
}

var expectedGatewayApplication domain.GatewayApplication = domain.GatewayApplication{
	SparkApplication: domain.GatewaySparkApplication{
		GatewayApplicationMeta: domain.GatewayApplicationMeta{
			Name:      "clusterid-nsid-uuid",
			Namespace: "testNamespace",
			Labels: map[string]string{
				domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
				domain.GATEWAY_USER_LABEL:    "user",
			},
			Annotations: map[string]string{
				domain.GATEWAY_APPLICATION_NAME_ANNOTATION: "appName",
			},
		},
		Spec: v1beta2.SparkApplicationSpec{
			ProxyUser: &TEST_USER,
		},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID:       "test123",
			SparkApplicationID: "sparkAppID",
		},
	},
	GatewayId: "clusterid-nsid-uuid",
	Cluster:   "test-cluster",
	User:      TEST_USER,
	SparkLogURLs: domain.SparkLogURLs{
		SparkUI:        "",
		SparkHistoryUI: "https://spark-history-testNamespace.test.com/history/sparkAppID/jobs",
		LogsUI:         "https://logs.test.com/app/discover#/?_g=(_a=(interval:auto,query:(language:lucene,query:'host:%20%22clusterid-nsid-uuid-driver%22')",
	},
}

var expectedSparkManagerSparkApplicationSummaries = []*domain.SparkManagerSparkApplicationSummary{
	{
		TypeMeta: v1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		GatewayApplicationMeta: domain.GatewayApplicationMeta{
			Name:      "clusterid-nsid-uuid",
			Namespace: "testNamespace",
			Labels: map[string]string{
				domain.GATEWAY_USER_LABEL:    TEST_USER,
				domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID: "test123",
		},
	},
	{
		TypeMeta: v1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		GatewayApplicationMeta: domain.GatewayApplicationMeta{
			Name:      "clusterid-nsid-uuid2",
			Namespace: "testNamespace",
			Labels: map[string]string{
				domain.GATEWAY_USER_LABEL:    TEST_USER,
				domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID: "test124",
		},
	},
}

var expectedGatewayApplicationSummaries []*domain.GatewayApplicationSummary = []*domain.GatewayApplicationSummary{
	{
		SparkManagerSparkApplicationSummary: domain.SparkManagerSparkApplicationSummary{
			TypeMeta: v1.TypeMeta{
				Kind:       "SparkApplication",
				APIVersion: "sparkoperator.k8s.io/v1beta2",
			},
			GatewayApplicationMeta: domain.GatewayApplicationMeta{
				Name:      "clusterid-nsid-uuid",
				Namespace: "testNamespace",
				Labels: map[string]string{
					domain.GATEWAY_USER_LABEL:    TEST_USER,
					domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
				},
			},
			Status: v1beta2.SparkApplicationStatus{
				SubmissionID: "test123",
			},
		},
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "test-cluster",
		User:      TEST_USER,
	},
	{
		SparkManagerSparkApplicationSummary: domain.SparkManagerSparkApplicationSummary{
			TypeMeta: v1.TypeMeta{
				Kind:       "SparkApplication",
				APIVersion: "sparkoperator.k8s.io/v1beta2",
			},
			GatewayApplicationMeta: domain.GatewayApplicationMeta{
				Name:      "clusterid-nsid-uuid2",
				Namespace: "testNamespace",
				Labels: map[string]string{
					domain.GATEWAY_USER_LABEL:    TEST_USER,
					domain.GATEWAY_CLUSTER_LABEL: "test-cluster",
				},
			},
			Status: v1beta2.SparkApplicationStatus{
				SubmissionID: "test124",
			},
		},
		GatewayId: "clusterid-nsid-uuid2",
		Cluster:   "test-cluster",
		User:      TEST_USER,
	},
}

var logString string = "testlogstring"

// SimpleClusterRouter
type SuccessClusterRouter struct{}

func (s *SuccessClusterRouter) GetCluster(ctx context.Context, namespace string) (*domain.KubeCluster, error) {
	return &testCluster, nil
}

// SimpleClusterRouter
type FailClusterRouter struct{}

func (s *FailClusterRouter) GetCluster(ctx context.Context, namespace string) (*domain.KubeCluster, error) {
	return nil, fmt.Errorf("no clusters with namespace %s returned", namespace)
}

// TestGatewayIdGenerator
func GatewayIdGenerator_Success(cluster domain.KubeCluster, namespace string) (string, error) {
	return "clusterid-nsid-uuid", nil
}

// TestGatewayIdGenerator
func GatewayIdGenerator_Failure(cluster domain.KubeCluster, namespace string) (string, error) {
	return "", errors.New("bad gatewayid")
}

var mockClusterRepo_Success repository.ClusterRepository = &repository.ClusterRepositoryMock{
	GetAllFunc: func() []domain.KubeCluster {
		return []domain.KubeCluster{testCluster}
	},
	GetAllWithNamespaceFunc: func(namespace string) []domain.KubeCluster {
		return []domain.KubeCluster{testCluster}
	},
	GetByIdFunc: func(clusterId string) (*domain.KubeCluster, error) {
		return &testCluster, nil
	},
	GetByNameFunc: func(cluster string) (*domain.KubeCluster, error) {
		return &testCluster, nil
	},
}

var mockClusterRepo_Failure repository.ClusterRepository = &repository.ClusterRepositoryMock{
	GetAllFunc: func() []domain.KubeCluster {
		return []domain.KubeCluster{}
	},
	GetAllWithNamespaceFunc: func(namespace string) []domain.KubeCluster {
		return []domain.KubeCluster{}
	},
	GetByIdFunc: func(clusterId string) (*domain.KubeCluster, error) {
		return nil, fmt.Errorf("cluster does not exist: %s", clusterId)
	},
	GetByNameFunc: func(cluster string) (*domain.KubeCluster, error) {
		return nil, fmt.Errorf("cluster does not exist: %s", cluster)
	},
}

var mockGatewayAppRepository_Success GatewayApplicationRepositoryMock = GatewayApplicationRepositoryMock{
	CreateFunc: func(ctx context.Context, cluster domain.KubeCluster, sparkApp *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return sparkApp, nil
	},
	DeleteFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) error {
		return nil
	},
	GetFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) (*v1beta2.SparkApplication, error) {
		return expectedSparkApp, nil
	},
	ListFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace string) ([]*domain.SparkManagerSparkApplicationSummary, error) {
		return expectedSparkManagerSparkApplicationSummaries, nil
	},
	LogsFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string, tailLines int) (*string, error) {
		return &logString, nil
	},
	StatusFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) (*v1beta2.SparkApplicationStatus, error) {
		return &expectedSparkApp.Status, nil
	},
}

var mockGatewayAppRepository_Failure GatewayApplicationRepositoryMock = GatewayApplicationRepositoryMock{
	CreateFunc: func(ctx context.Context, cluster domain.KubeCluster, sparkApp *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return nil, errors.New("error creating GatewayApplication")
	},
	DeleteFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) error {
		return errors.New("error deleting SparkApp")
	},
	GetFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) (*v1beta2.SparkApplication, error) {
		return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting GatewayApplication '%s/%s'", namespace, name))
	},
	ListFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace string) ([]*domain.SparkManagerSparkApplicationSummary, error) {
		return nil, errors.New("error getting application summaries:")
	},
	LogsFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string, tailLines int) (*string, error) {
		return nil, errors.New("error getting logs")
	},
	StatusFunc: func(ctx context.Context, cluster domain.KubeCluster, namespace, name string) (*v1beta2.SparkApplicationStatus, error) {
		return nil, errors.New("error getting application status:")
	},
}

func TestServiceGet(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)
	gatewayApp, _ := appService.Get(context.Background(), "clusterid-nsid-uuid")
	assert.Equal(t, &expectedGatewayApplication, gatewayApp, "returned GatewayApplication should match")
}

func TestServiceGetNotFound(t *testing.T) {

	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)
	gatewayApp, err := appService.Get(context.Background(), "clusterid-nsid-uuid")

	assert.Equal(t, (*domain.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting GatewayApplication 'testNamespace/clusterid-nsid-uuid'", "error should match")

}

func TestList(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	summaries, err := appService.List(context.Background(), "test-cluster", "testNamespace")

	assert.Equal(t, expectedGatewayApplicationSummaries, summaries, "returned GatewayApplication should match")
	assert.Equal(t, nil, err, "err should be nil")

}

func TestListClusterFail(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Failure,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	summaries, err := appService.List(context.Background(), "test-cluster", "testNamespace")

	assert.Equal(t, []*domain.GatewayApplicationSummary(nil), summaries, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting cluster:", "err should match")

}

func TestListAppRepoFail(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	summaries, err := appService.List(context.Background(), "test-cluster", "testNamespace")

	assert.Nil(t, summaries, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting applications:", "err should match")

}

func TestServiceCreateClusterFail(t *testing.T) {

	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&FailClusterRouter{},
		&FailClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	gatewayApp, err := appService.Create(context.Background(), inputSparkApp, TEST_USER)

	assert.Nil(t, gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting routing cluster:", "err should match")
}

func TestServiceCreateGenIDFail(t *testing.T) {

	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	gatewayApp, err := appService.Create(context.Background(), inputSparkApp, TEST_USER)

	assert.Nil(t, gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error generating GatewayId for GatewayApplication:", "err should match")
}

func TestServiceCreateRepoFail(t *testing.T) {

	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	gatewayApp, err := appService.Create(context.Background(), inputSparkApp, TEST_USER)

	assert.Nil(t, gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error creating GatewayApplication 'testNamespace/clusterid-nsid-uuid':", "err should match")
}

func TestServiceCreateRepoSuccess(t *testing.T) {

	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	gatewayApp, err := appService.Create(context.Background(), inputSparkApp, TEST_USER)

	assert.Equal(t, &expectedGatewayApplication, gatewayApp, "returned GatewayApplication should match")
	assert.Nil(t, err, "err should be nil")
}

func TestServiceCreateRoutingError(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&FailClusterRouter{},
		&FailClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Success,
	)

	inApp := v1beta2.SparkApplication{}

	gatewayApp, err := appService.Create(context.Background(), &inApp, TEST_USER)

	assert.Equal(t, (*domain.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting routing cluster:", "err should match")
}

func TestServiceCreateGatewayIdGenError(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	inApp := v1beta2.SparkApplication{}

	gatewayApp, err := appService.Create(context.Background(), &inApp, TEST_USER)

	assert.Equal(t, (*domain.GatewayApplication)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error generating GatewayId for GatewayApplication:", "err should match")
}

func TestServiceStatus(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	gotStatus, _ := appService.Status(context.Background(), "clusterid-nsid-uuid")

	assert.Equal(t, &expectedGatewayApplication.SparkApplication.Status, gotStatus, "returned response should match")
}

func TestServiceBadStatus(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	gatewayApp, err := appService.Status(context.Background(), "clusterid-nsid-uuid")

	assert.Equal(t, (*v1beta2.SparkApplicationStatus)(nil), gatewayApp, "returned GatewayApplication should be nil")
	assert.Contains(t, err.Error(), "error getting status for GatewayApplication", "err should match")
}

func TestServiceLogs(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Success,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	gatewayLogs, _ := appService.Logs(context.Background(), "clusterid-nsid-uuid", 100)

	assert.Equal(t, &logString, gatewayLogs, "returned Gateway logs should be same")
}

func TestServiceBadLogs(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)

	gatewayLogs, err := appService.Logs(context.Background(), "clusterid-nsid-uuid", 100)

	assert.Equal(t, (*string)(nil), gatewayLogs, "returned logs should be nil")
	assert.Contains(t, err.Error(), "error getting logs for GatewayApplication", "err should match")
}

func TestServiceDeleteError(t *testing.T) {
	appService := NewApplicationService(
		&mockGatewayAppRepository_Failure,
		mockClusterRepo_Success,
		&SuccessClusterRouter{},
		&SuccessClusterRouter{},
		testGatewayConfig,
		"",
		"",
		GatewayIdGenerator_Failure,
	)
	assert.Contains(t, appService.Delete(context.Background(), "clusterid-nsid-uuid").Error(), "error deleting GatewayApplication 'clusterid-nsid-uuid': error deleting SparkApp", "errors should match")

}

func TestRenderURLs(t *testing.T) {
	urlTemplates := domain.StatusUrlTemplates{
		SparkUITemplate:        "host.com/ui/{{.Namespace}}/{{.Name}}",
		SparkHistoryUITemplate: "host.com/history/ui/{{.Namespace}}/{{.Name}}",
		LogsUITemplate:         "host.com/logs/ui/{{.Namespace}}/{{.Name}}",
	}

	gaSparkApp := domain.GatewaySparkApplication{
		GatewayApplicationMeta: domain.GatewayApplicationMeta{
			Name:      "clusterid-nsid-uuid",
			Namespace: "namespace",
		},
	}

	expected := domain.SparkLogURLs{
		SparkUI:        "host.com/ui/namespace/clusterid-nsid-uuid",
		SparkHistoryUI: "host.com/history/ui/namespace/clusterid-nsid-uuid",
		LogsUI:         "host.com/logs/ui/namespace/clusterid-nsid-uuid",
	}

	assert.Equal(t, expected, GetRenderedURLs(urlTemplates, &gaSparkApp))
}
