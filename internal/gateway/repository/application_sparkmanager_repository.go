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
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
	"github.com/slackhq/spark-gateway/internal/shared/util"
)

type SparkManagerRepository struct {
	ClusterEndpoints map[string]string
}

func NewSparkManagerRepository(clusters []domain.KubeCluster, sparkManagerHostnameTemplate string, sparkManagerPort string, debugPorts map[string]config.DebugPort) (*SparkManagerRepository, error) {

	hostNameF := "http://%s:%s/api/v1"
	clusterEndpoints := map[string]string{}

	// Pretemplate the hostname with debug port if any
	for _, kubeCluster := range clusters {
		// Set sparkManager port
		sparkManagerPort := sparkManagerPort
		if debugPort, ok := debugPorts[kubeCluster.Name]; ok {
			sparkManagerPort = debugPort.SparkManagerPort
		}

		// Template hostname
		hostname, err := util.RenderTemplate(sparkManagerHostnameTemplate, map[string]string{"clusterName": kubeCluster.Name})

		if err != nil {
			return nil, fmt.Errorf("error while formatting SparkManager Hostname: %w", err)
		}

		// Create full hostname
		hostNamePort := fmt.Sprintf(hostNameF, *hostname, sparkManagerPort)

		clusterEndpoints[kubeCluster.Name] = hostNamePort
		klog.Infof("Cluster %s configured with endpoint: %s", kubeCluster.Name, hostNamePort)

	}

	return &SparkManagerRepository{
		ClusterEndpoints: clusterEndpoints,
	}, nil
}

func (r *SparkManagerRepository) Get(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) (*domain.GatewayApplication, error) {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace/name
	url := fmt.Sprintf("%s/%s/%s", clusterEndpoint, namespace, name)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
	}

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	var sparkApp v1beta2.SparkApplication
	if err := json.Unmarshal(*respBody, &sparkApp); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal JSON response: %w", err)
	}

	return domain.NewGatewayApplication(sparkApp), nil
}

func (r *SparkManagerRepository) List(ctx context.Context, cluster domain.KubeCluster, namespace string) ([]*domain.GatewayApplication, error) {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace
	url := fmt.Sprintf("%s/%s", clusterEndpoint, namespace)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
	}

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	var sparkAppList []*v1beta2.SparkApplication
	if err := json.Unmarshal(*respBody, &sparkAppList); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal JSON response: %w", err)
	}

	var gatewayAppList []*domain.GatewayApplication
	for _, sparkApp := range sparkAppList {
		gatewayApp := domain.NewGatewayApplication(*sparkApp)
		gatewayAppList = append(gatewayAppList, gatewayApp)
	}

	return gatewayAppList, nil
}

func (r *SparkManagerRepository) Status(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) (*domain.GatewayApplicationStatus, error) {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace/name/status
	url := fmt.Sprintf("%s/%s/%s/status", clusterEndpoint, namespace, name)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
	}

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	var appStatus v1beta2.SparkApplicationStatus
	if err := json.Unmarshal(*respBody, &appStatus); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal JSON response: %w", err)
	}

	return domain.NewGatewayApplicationStatus(appStatus), nil
}

func (r *SparkManagerRepository) Logs(ctx context.Context, cluster domain.KubeCluster, namespace string, name string, tailLines int) (*string, error) {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace/name/logs?lines=lineCount
	url := fmt.Sprintf("%s/%s/%s/logs?lines=%d", clusterEndpoint, namespace, name, tailLines)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
	}

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	var logString string
	if err := json.Unmarshal(*respBody, &logString); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal JSON response: %w", err)
	}

	return &logString, nil
}

func (r *SparkManagerRepository) Create(ctx context.Context, cluster domain.KubeCluster, sparkApplication *v1beta2.SparkApplication) (*domain.GatewayApplication, error) {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace/name
	url := fmt.Sprintf("%s/%s/%s", clusterEndpoint, sparkApplication.Namespace, sparkApplication.Name)

	body, err := json.Marshal(sparkApplication)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SparkApplication: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodPost, err))
	}
	request.Header.Set("Content-Type", "application/json")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	var sparkApp v1beta2.SparkApplication
	if err := json.Unmarshal(*respBody, &sparkApp); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal JSON response: %w", err)
	}

	return domain.NewGatewayApplication(sparkApp), nil
}

func (r *SparkManagerRepository) Delete(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) error {

	clusterEndpoint := r.ClusterEndpoints[cluster.Name]
	// Url: http://host:port/api/v1/namespace/name
	url := fmt.Sprintf("%s/%s/%s", clusterEndpoint, namespace, name)

	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", http.MethodDelete, err))
	}

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return gatewayerrors.NewFrom(err)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return gatewayerrors.NewFrom(err)
	}

	return nil
}
