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

package domain

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/shared/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const GATEWAY_USER_LABEL = "spark-gateway/user"

type StatusUrlTemplates struct {
	SparkUI        string `koanf:"sparkUI"`
	SparkHistoryUI string `koanf:"sparkHistoryUI"`
	LogsUI         string `koanf:"logsUI"`
}

// Most models here are simply wrappers for corresponding v1beta2 types to help with "decoupling". These will most likely need
// to be expanded into individual models like what Batch Processing Gateway did to fully decouple everything, but since we're
// focusing on Kubeflow Spark Operator for now, we will target their models
type GatewayApplicationStatus struct {
	v1beta2.SparkApplicationStatus
}

func NewGatewayApplicationStatus(status v1beta2.SparkApplicationStatus) *GatewayApplicationStatus {
	gatewayStatus := &GatewayApplicationStatus{
		SparkApplicationStatus: status,
	}

	gatewayStatus.ExecutorState = nil

	return gatewayStatus
}

type GatewayApplicationSpec struct {
	v1beta2.SparkApplicationSpec
}

type GatewayApplicationMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

func NewGatewayApplicationMeta(appMeta metav1.ObjectMeta) *GatewayApplicationMeta {
	return &GatewayApplicationMeta{
		Name:        appMeta.Name,
		Namespace:   appMeta.Namespace,
		Annotations: appMeta.Annotations,
		Labels:      appMeta.Labels,
	}
}

type GatewayApplicationSummary struct {
	GatewayApplicationMeta   `json:",inline"`
	GatewayApplicationStatus `json:"status"`
}

func NewGatewayApplicationSummary(sparkApp v1beta2.SparkApplication) *GatewayApplicationSummary {
	return &GatewayApplicationSummary{
		GatewayApplicationMeta:   *NewGatewayApplicationMeta(sparkApp.ObjectMeta),
		GatewayApplicationStatus: *NewGatewayApplicationStatus(sparkApp.Status),
	}
}

type GatewayApplication struct {
	GatewayApplicationMeta `json:"metadata"`
	Spec                   GatewayApplicationSpec   `json:"spec"`
	Status                 GatewayApplicationStatus `json:"status"`
	GatewayId              string                   `json:"gatewayId"`
	Cluster                string                   `json:"cluster"`
	User                   string                   `json:"user"`
	SparkLogURLs           StatusUrlTemplates       `json:"sparkLogURLs"`
}

// NewGatewayApplication will return a new GatewayApplication by first mapping the input v1beta2.SparkApplication and then applying any
// opt functions to the mapped application
func NewGatewayApplication(sparkApp *v1beta2.SparkApplication, opts ...func(*GatewayApplication)) *GatewayApplication {

	// Default labels and annotations
	annotations := map[string]string{}
	labels := map[string]string{}
	if sparkApp.Annotations != nil {
		annotations = sparkApp.Annotations
	}

	if sparkApp.Labels != nil {
		labels = sparkApp.Labels
	}

	status := GatewayApplicationStatus{SparkApplicationStatus: sparkApp.Status}
	status.ExecutorState = nil

	gatewayApp := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace:   sparkApp.Namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Spec:   GatewayApplicationSpec{SparkApplicationSpec: sparkApp.Spec},
		Status: status,
	}

	// If the application already has a name, we set it as an annotation because
	// all GatewayApplication names are GatewayIds
	if sparkApp.ObjectMeta.Name != "" {
		gatewayApp.Annotations["applicationName"] = sparkApp.Name
	}

	// Apply opts
	for _, o := range opts {
		o(&gatewayApp)
	}

	return &gatewayApp

}

func WithUser(user string) func(*GatewayApplication) {
	return func(ga *GatewayApplication) {
		ga.User = user
		ga.Labels[GATEWAY_USER_LABEL] = user
		ga.Spec.ProxyUser = &user
	}
}

func WithSelector(selectorMap map[string]string) func(*GatewayApplication) {
	return func(ga *GatewayApplication) {
		// Add selector values if they exist
		if len(selectorMap) != 0 {
			ga.Labels = util.MergeMaps(ga.Labels, selectorMap)
		}
	}
}

func WithId(gatewayId string) func(*GatewayApplication) {
	return func(ga *GatewayApplication) {
		ga.GatewayId = gatewayId
		ga.Name = gatewayId
	}
}

func NewId(cluster KubeCluster, namespace string) (string, error) {
	// Generate name from clusterId, namespaceId, and UUID
	uuid, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("error generating application UUID: %w", err)
	}

	kubeNamespace, err := cluster.GetNamespaceByName(namespace)
	if err != nil {
		return "", fmt.Errorf("error generating GatewayId: %w", err)
	}

	appName := fmt.Sprintf("%s-%s-%s", cluster.ClusterId, kubeNamespace.NamespaceId, uuid)

	return appName, nil
}

func ParseGatewayIdUUID(gatewayId string) (*uuid.UUID, error) {
	parts := strings.Split(gatewayId, "-")
	if len(parts) == 7 {
		uid, err := uuid.Parse(strings.Join(parts[2:], "-"))
		if err != nil {
			return nil, fmt.Errorf("error parsing gateway UUID (%s): %v", gatewayId, err)
		}
		return &uid, nil
	}
	return nil, fmt.Errorf("error parsing gatewayId (%s). Format must be 'cluster-namespace-uuid'", gatewayId)
}
