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
const GATEWAY_CLUSTER_LABEL = "spark-gateway/cluster"
const GATEWAY_APPLICATION_NAME_ANNOTATION = "applicationName"

type StatusUrlTemplates struct {
	SparkUITemplate        string `koanf:"sparkUI"`
	SparkHistoryUITemplate string `koanf:"sparkHistoryUI"`
	LogsUITemplate         string `koanf:"logsUI"`
}

type SparkLogURLs struct {
	SparkUI        string `json:"sparkUI"`
	SparkHistoryUI string `json:"sparkHistoryUI"`
	LogsUI         string `json:"logsUI"`
}

// Most models here are simply wrappers for corresponding v1beta2 types with some fields removed or defaulted. These will most likely need
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
	Kind        string            `json:"kind"`
	APIVersion  string            `json:"apiVersion"`
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

func NewGatewayApplicationMeta(appMeta metav1.ObjectMeta, appType metav1.TypeMeta) *GatewayApplicationMeta {
	// Default labels and annotations because these can be nil
	annotations := map[string]string{}
	labels := map[string]string{}
	if appMeta.Annotations != nil {
		annotations = appMeta.Annotations
	}

	if appMeta.Labels != nil {
		labels = appMeta.Labels
	}

	return &GatewayApplicationMeta{
		Kind:        appType.Kind,
		APIVersion:  appType.APIVersion,
		Name:        appMeta.Name,
		Namespace:   appMeta.Namespace,
		Annotations: annotations,
		Labels:      labels,
	}
}

type GatewayApplicationSummary struct {
	GatewayApplicationMeta   `json:",inline"`
	GatewayApplicationStatus `json:"status"`
	GatewayId                string `json:"gatewayId"`
	Cluster                  string `json:"cluster"`
}

func NewGatewayApplicationSummary(sparkApp *v1beta2.SparkApplication, cluster string) *GatewayApplicationSummary {
	return &GatewayApplicationSummary{
		// Name should always be GatewayId due to model mappings etc.
		GatewayApplicationMeta:   *NewGatewayApplicationMeta(sparkApp.ObjectMeta, sparkApp.TypeMeta),
		GatewayApplicationStatus: *NewGatewayApplicationStatus(sparkApp.Status),
		GatewayId:                sparkApp.Name,
		Cluster:                  cluster,
	}
}

type GatewaySparkApplication struct {
	GatewayApplicationMeta `json:"metadata"`
	Spec                   GatewayApplicationSpec   `json:"spec"`
	Status                 GatewayApplicationStatus `json:"status"`
}

func (gsa *GatewaySparkApplication) ToV1Beta2SparkApplication() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		TypeMeta: metav1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        gsa.Name,
			Namespace:   gsa.Namespace,
			Annotations: gsa.Annotations,
			Labels:      gsa.Labels,
		},
		Spec:   gsa.Spec.SparkApplicationSpec,
		Status: gsa.Status.SparkApplicationStatus,
	}
}

func NewGatewaySparkApplication(sparkApp *v1beta2.SparkApplication, opts ...func(*GatewaySparkApplication)) *GatewaySparkApplication {

	gaSparkApp := &GatewaySparkApplication{
		GatewayApplicationMeta: *NewGatewayApplicationMeta(sparkApp.ObjectMeta, sparkApp.TypeMeta),
		Spec:                   GatewayApplicationSpec{SparkApplicationSpec: sparkApp.Spec},
		Status:                 *NewGatewayApplicationStatus(sparkApp.Status),
	}

	// Apply opts
	for _, o := range opts {
		o(gaSparkApp)
	}

	return gaSparkApp

}

func GatewaySparkApplicationFromV1Beta2SparkApplication(sparkApp v1beta2.SparkApplication) *GatewaySparkApplication {
	return &GatewaySparkApplication{
		GatewayApplicationMeta: *NewGatewayApplicationMeta(sparkApp.ObjectMeta, sparkApp.TypeMeta),
		Spec:                   GatewayApplicationSpec{SparkApplicationSpec: sparkApp.Spec},
		Status:                 *NewGatewayApplicationStatus(sparkApp.Status),
	}
}

type GatewayApplication struct {
	SparkApplication GatewaySparkApplication `json:"sparkApplication"`
	GatewayId        string                  `json:"gatewayId"`
	Cluster          string                  `json:"cluster"`
	User             string                  `json:"user"`
	SparkLogURLs     SparkLogURLs            `json:"sparkLogURLs"`
}

func GatewayApplicationFromV1Beta2SparkApplication(sparkApp *v1beta2.SparkApplication) *GatewayApplication {
	gatewayId := sparkApp.Name
	appUser := sparkApp.Labels[GATEWAY_USER_LABEL]
	cluster := sparkApp.Labels[GATEWAY_CLUSTER_LABEL]

	return &GatewayApplication{
		SparkApplication: *GatewaySparkApplicationFromV1Beta2SparkApplication(*sparkApp),
		GatewayId:        gatewayId,
		Cluster:          cluster,
		User:             appUser,
	}
}

func WithUser(user string) func(*GatewaySparkApplication) {
	return func(gsa *GatewaySparkApplication) {
		gsa.Labels[GATEWAY_USER_LABEL] = user
		gsa.Spec.ProxyUser = &user
	}
}

func WithCluster(cluster string) func(*GatewaySparkApplication) {
	return func(gsa *GatewaySparkApplication) {
		gsa.Labels[GATEWAY_CLUSTER_LABEL] = cluster
	}
}

func WithSelector(selectorMap map[string]string) func(*GatewaySparkApplication) {
	return func(gsa *GatewaySparkApplication) {
		// Add selector values if they exist
		if len(selectorMap) != 0 {
			gsa.Labels = util.MergeMaps(gsa.Labels, selectorMap)
		}
	}
}

func WithId(gatewayId string) func(*GatewaySparkApplication) {
	return func(gsa *GatewaySparkApplication) {
		// If the application already has a name, we set it as an annotation because
		// all GatewayApplication names are GatewayIds
		if gsa.Name != "" {
			gsa.Annotations[GATEWAY_APPLICATION_NAME_ANNOTATION] = gsa.Name
		}

		gsa.Name = gatewayId
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
