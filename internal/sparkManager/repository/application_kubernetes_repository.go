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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	"github.com/slackhq/spark-gateway/internal/shared/util"
	"github.com/slackhq/spark-gateway/internal/sparkManager/kube"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkClientSet "github.com/kubeflow/spark-operator/v2/pkg/client/clientset/versioned"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type SparkApplicationRepository struct {
	sparkClient *sparkClientSet.Clientset
	k8sClient   *kubernetes.Clientset
	controller  kube.SparkController
}

func NewSparkApplicationRepository(controller *kube.SparkController, sparkClient *sparkClientSet.Clientset, k8sClient *kubernetes.Clientset) (*SparkApplicationRepository, error) {
	return &SparkApplicationRepository{
		sparkClient: sparkClient,
		k8sClient:   k8sClient,
		controller:  *controller,
	}, nil
}

func (s *SparkApplicationRepository) Get(namespace string, name string) (*v1beta2.SparkApplication, error) {

	sparkApp, err := s.controller.SparkLister.SparkApplications(namespace).Get(name)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error getting SparkApplication '%s/%s': %w", namespace, name, err))
	}

	return sparkApp, nil

}

func (s *SparkApplicationRepository) List(namespace string) ([]*v1beta2.SparkApplication, error) {

	sparkApps, err := s.controller.SparkLister.SparkApplications(namespace).List(labels.Everything())

	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error listing SparkApplications in namespace [%s]: %w", namespace, err))
	}

	return sparkApps, nil

}

func (s *SparkApplicationRepository) GetLogs(namespace string, name string, tailLines int64) (*string, error) {

	sparkApp, err := s.Get(namespace, name)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error getting SparkApplication '%s/%s' to get Spark Driver Pod name for logs: %w", sparkApp.Namespace, sparkApp.Name, err))
	}
	logString, err := util.GetLogs(sparkApp.Status.DriverInfo.PodName, sparkApp.Namespace, tailLines, s.k8sClient)
	if err != nil {
		return nil, err
	}
	logLines := util.UnmarshalLogLines(*logString)
	formattedLogString := util.FormatLogLines(logLines)

	return formattedLogString, nil
}

func (s *SparkApplicationRepository) Create(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {

	sparkApp, err := s.sparkClient.SparkoperatorV1beta2().SparkApplications(application.Namespace).Create(ctx, application, v1.CreateOptions{})
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error creating SparkApplication: %w", err))
	}

	counter := 5
	for counter > 0 {
		sparkApp, err = s.Get(application.Namespace, application.Name)
		if err != nil {
			if getErr, ok := err.(gatewayerrors.GatewayError); ok {
				if getErr.Status == http.StatusNotFound {
					continue
				}
				return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error getting SparkApplication after create: %w", err))
			}
			return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error getting SparkApplication after create: %w", err))
		}

		if sparkApp.ObjectMeta.UID != "" {
			break
		}
		counter -= 1
		time.Sleep(1 * time.Second)
	}

	if sparkApp.ObjectMeta.UID == "" {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error getting ObjectMeta.UID from the submitted SparkApplication"))
	}

	return sparkApp, nil
}

func (s *SparkApplicationRepository) Delete(ctx context.Context, namespace string, name string) error {
	if err := s.sparkClient.SparkoperatorV1beta2().SparkApplications(namespace).Delete(ctx, name, v1.DeleteOptions{}); err != nil {
		return gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error deleting SparkApplication: %w", err))
	}

	return nil
}
