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
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
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

type GatewayApplicationSpec struct {
	v1beta2.SparkApplicationSpec
}

type GatewayApplicationMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
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

// SetUser will add the Gateway user label to the apps Labels and override
// the Spec ProxyUser
func (g *GatewayApplication) SetUser(user string) {

	// Add user label to app Labels
	g.Labels[GATEWAY_USER_LABEL] = user

	// Set ProxyUser
	g.Spec.ProxyUser = &user
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

// GatewayApplicationFromV1Beta2Application will return a new GatewayApplication by mapping inputs from a v1beta2.SparkApplication
// and setting defaults.
func GatewayApplicationFromV1Beta2Application(sparkApp v1beta2.SparkApplication) (*GatewayApplication, error) {

	if sparkApp.Namespace == "" {
		return nil, errors.New("submitted SparkApplication must have a Namespace")
	}

	// Create base structs first
	meta := GatewayApplicationMeta{
		Namespace: sparkApp.Namespace,
	}
	spec := GatewayApplicationSpec{SparkApplicationSpec: sparkApp.Spec}
	status := GatewayApplicationStatus{SparkApplicationStatus: sparkApp.Status}

	// Default labels and annotations
	annotations := map[string]string{}
	labels := map[string]string{}
	if sparkApp.Annotations != nil {
		annotations = sparkApp.Annotations
	}

	if sparkApp.Labels != nil {
		labels = sparkApp.Labels
	}

	meta.Annotations = annotations
	meta.Labels = labels

	// If the application already has a name, we set it as an annotation because
	// gateway will later override it with a GatewayId
	if sparkApp.Name != "" {
		annotations["applicationName"] = sparkApp.Name
	}

	return &GatewayApplication{
		GatewayApplicationMeta: meta,
		Spec:                   spec,
		Status:                 status,
	}, nil

}
