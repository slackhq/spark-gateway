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

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/model"
)

var expectedSparkApplication v1beta2.SparkApplication = v1beta2.SparkApplication{
	ObjectMeta: v1.ObjectMeta{
		Name:      "clusterid-nsid-testid",
		Namespace: "testNamespace",
	},
	Status: v1beta2.SparkApplicationStatus{
		SubmissionID: "test123",
	}}

var logString string = "testlogstring"

var testCluster model.KubeCluster = model.KubeCluster{
	Name:      "test-cluster",
	MasterURL: "masterUrl",
	ClusterId: "id",
	Namespaces: []model.KubeNamespace{
		{
			Name:        "testNamespace",
			NamespaceId: "nsid",
		},
	},
}

var mockSparkAppRepository_SuccessTests SparkApplicationRepositoryMock = SparkApplicationRepositoryMock{
	GetFunc: func(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error) {
		return &expectedSparkApplication, nil
	},
	GetLogsFunc: func(ctx context.Context, namespace string, name string, tailLine int64) (*string, error) {
		return &logString, nil
	},
	CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return &expectedSparkApplication, nil
	},
	DeleteFunc: func(ctx context.Context, namespace string, name string) error {
		return nil
	},
}

var mockSparkAppRepository_FailureTests SparkApplicationRepositoryMock = SparkApplicationRepositoryMock{
	GetFunc: func(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error) {
		return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s/%s'", expectedSparkApplication.Namespace, expectedSparkApplication.Name))
	},
	GetLogsFunc: func(ctx context.Context, namespace string, name string, tailLine int64) (*string, error) {
		return nil, errors.New("error getting logs")
	},
	CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return nil, errors.New("error creating SparkApp")
	},
	DeleteFunc: func(ctx context.Context, namespace string, name string) error {
		return errors.New("error deleting SparkApp")
	},
}

func TestSparkApplicationService_Get(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_SuccessTests, nil, testCluster)

	result, err := service.Get(context.Background(), "testNamespace", "clusterid-nsid-testid")
	assert.NoError(t, err)
	assert.Equal(t, &expectedSparkApplication, result)
}

func TestSparkApplicationService_Get_Error(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_FailureTests, nil, testCluster)

	_, err := service.Get(context.Background(), "testNamespace", "clusterid-nsid-testid")
	assert.Error(t, err)
	assert.Equal(t, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s/%s'", expectedSparkApplication.Namespace, expectedSparkApplication.Name)), err)
}

func TestSparkApplicationService_Status(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_SuccessTests, nil, testCluster)

	result, err := service.Get(context.Background(), "testNamespace", "clusterid-nsid-testid")
	assert.NoError(t, err)
	assert.Equal(t, &expectedSparkApplication, result)
}

func TestSparkApplicationService_Status_Error(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_FailureTests, nil, testCluster)

	_, err := service.Get(context.Background(), "testNamespace", "clusterid-nsid-testid")
	assert.Error(t, err)
	assert.Equal(t, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s/%s'", expectedSparkApplication.Namespace, expectedSparkApplication.Name)), err)
}

func TestSparkApplicationService_GetLogs(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_SuccessTests, nil, testCluster)

	result, err := service.Logs(context.Background(), "testNamespace", "clusterid-nsid-testid", 100)
	assert.NoError(t, err)
	assert.Equal(t, &logString, result)
}

func TestSparkApplicationService_GetLogs_Error(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_FailureTests, nil, testCluster)

	_, err := service.Logs(context.Background(), "testNamespace", "clusterid-nsid-testid", 100)
	assert.Error(t, err)
	assert.Equal(t, errors.New("error getting logs"), err)
}

func TestSparkApplicationService_Create(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_SuccessTests, nil, testCluster)

	result, err := service.Create(context.Background(), &expectedSparkApplication)
	assert.NoError(t, err)
	assert.Equal(t, &expectedSparkApplication, result)
}

func TestSparkApplicationService_Create_Error(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_FailureTests, nil, testCluster)

	_, err := service.Create(context.Background(), &expectedSparkApplication)

	assert.Error(t, err)
	assert.Equal(t, gatewayerrors.NewFrom(errors.New("error creating SparkApp")), err)
}

func TestSparkApplicationService_Delete(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_SuccessTests, nil, testCluster)

	err := service.Delete(context.Background(), "testNamespace", "clusterid-nsid-testid")
	assert.NoError(t, err)
}

func TestSparkApplicationService_Delete_Error(t *testing.T) {
	service := NewSparkApplicationService(&mockSparkAppRepository_FailureTests, nil, testCluster)

	err := service.Delete(context.Background(), "testNamespace", "clusterid-nsid-testid")

	assert.Error(t, err)
	assert.Equal(t, gatewayerrors.NewInternal(errors.New("error deleting SparkApp")), err)
}
