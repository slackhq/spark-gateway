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
	"time"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/sparkManager/application/handler"
	"github.com/slackhq/spark-gateway/pkg/database/repository"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/model"
)

//go:generate moq -rm -out mocksparkapplicationrepository.go . SparkApplicationRepository

type SparkApplicationRepository interface {
	Get(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error)
	List(ctx context.Context, namespace string) ([]*metav1.ObjectMeta, error)
	GetLogs(ctx context.Context, namespace string, name string, tailLines int64) (*string, error)
	Create(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error)
	Delete(ctx context.Context, namespace string, name string) error
}

type SparkApplicationService struct {
	sparkApplicationRepository SparkApplicationRepository
	database                   repository.DatabaseRepository
	cluster                    model.KubeCluster
}

func NewSparkApplicationService(sparkAppRepo SparkApplicationRepository, database repository.DatabaseRepository, cluster model.KubeCluster) handler.SparkApplicationService {
	return &SparkApplicationService{sparkApplicationRepository: sparkAppRepo, database: database, cluster: cluster}
}

func (s *SparkApplicationService) Get(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error) {

	sparkApp, err := s.sparkApplicationRepository.Get(ctx, namespace, name)

	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	return sparkApp, nil
}

func (s *SparkApplicationService) List(ctx context.Context, namespace string) ([]*metav1.ObjectMeta, error) {

	sparkApp, err := s.sparkApplicationRepository.List(ctx, namespace)

	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	return sparkApp, nil
}

func (s *SparkApplicationService) Status(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplicationStatus, error) {
	sparkApp, err := s.Get(ctx, namespace, name)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	return &sparkApp.Status, nil
}

func (s *SparkApplicationService) Logs(ctx context.Context, namespace string, name string, tailLines int64) (*string, error) {
	return s.sparkApplicationRepository.GetLogs(ctx, namespace, name, tailLines)
}

func (s *SparkApplicationService) Create(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {

	if s.database != nil {
		uid, err := model.ParseGatewayIdUUID(application.Name)
		if err != nil {
			klog.ErrorS(err, "Failed to parse the gateway UUID, unable to insert into DB", "gatewayId", application.Name)
			return nil, gatewayerrors.NewFrom(err)
		}
		err = s.database.InsertSparkApplication(ctx, *uid, time.Now().UTC(), application, s.cluster.Name)
		if err != nil {
			klog.Errorf("error inserting SparkApplication into database: %s", err.Error())
			return nil, gatewayerrors.NewFrom(err)
		}
	}

	sparkApp, err := s.sparkApplicationRepository.Create(ctx, application)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	return sparkApp, nil
}

func (s *SparkApplicationService) Delete(ctx context.Context, namespace string, name string) error {
	if err := s.sparkApplicationRepository.Delete(ctx, namespace, name); err != nil {
		return gatewayerrors.NewFrom(err)
	}

	return nil
}
