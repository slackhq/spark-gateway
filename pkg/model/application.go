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

package model

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const GATEWAY_USER_LABEL = "spark-gateway/user"

type SparkLogURLs struct {
	SparkUI        string `json:"sparkUI"`
	LogsUI         string `json:"logsUI"`
	SparkHistoryUI string `json:"sparkHistoryUI"`
}

type StatusUrlTemplates struct {
	SparkUI        string `koanf:"sparkUI"`
	SparkHistoryUI string `koanf:"sparkHistoryUI"`
	LogsUI         string `koanf:"logsUI"`
}

type GatewayApplication struct {
	*v1beta2.SparkApplication `json:"sparkApplication"`
	GatewayId                 string       `json:"gatewayId"`
	Cluster                   string       `json:"cluster"`
	User                      string       `json:"user"`
	SparkLogURLs              SparkLogURLs `json:"sparkLogURLs"`
}

type GatewayIdGenerator struct {
	UuidGenerator func() (string, error)
}

func (g *GatewayIdGenerator) NewId(cluster KubeCluster, namespace string) (string, error) {
	// Generate name from clusterId and UUID and set
	genUUID, err := g.UuidGenerator()
	if err != nil {
		return "", fmt.Errorf("error generating application UUID: %w", err)
	}

	kubeNamespace, err := cluster.GetNamespaceByName(namespace)
	if err != nil {
		return "", fmt.Errorf("error generating GatewayId: %w", err)
	}

	appName := fmt.Sprintf("%s-%s-%s", cluster.ClusterId, kubeNamespace.NamespaceId, genUUID)

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

type SparkManagerApplicationMeta struct {
	ObjectMeta metav1.ObjectMeta `json:"meta"`
	// All fields below come from v1beta2.SparkApplicationStatus
	// Currently it's all fields except for ExecutorState because it can get pretty large
	SparkApplicationID        string                   `json:"sparkApplicationId"`
	SubmissionID              string                   `json:"submissionID"`
	LastSubmissionAttemptTime metav1.Time              `json:"lastSubmissionAttemptTime"`
	TerminationTime           metav1.Time              `json:"terminationTime"`
	DriverInfo                v1beta2.DriverInfo       `json:"driverInfo"`
	AppState                  v1beta2.ApplicationState `json:"applicationState"`
	ExecutionAttempts         int32                    `json:"executionAttempts"`
	SubmissionAttempts        int32                    `json:"submissionAttempts"`
}

type GatewayApplicationMeta struct {
	SparkManagerApplicationMeta SparkManagerApplicationMeta
	Cluster                     string `json:"cluster"`
}

func NewSparkManagerApplicationMeta(sparkApp *v1beta2.SparkApplication) *SparkManagerApplicationMeta {

	if sparkApp == nil {
		return nil
	}

	// managedFields can be large and is not a required field
	sparkApp.ManagedFields = nil
	return &SparkManagerApplicationMeta{
		ObjectMeta: sparkApp.ObjectMeta,
		// All fields below come from v1beta2.SparkApplicationStatus
		// Currently it's all fields except for ExecutorState because it can get pretty large
		SparkApplicationID:        sparkApp.Status.SparkApplicationID,
		SubmissionID:              sparkApp.Status.SubmissionID,
		LastSubmissionAttemptTime: sparkApp.Status.LastSubmissionAttemptTime,
		TerminationTime:           sparkApp.Status.TerminationTime,
		DriverInfo:                sparkApp.Status.DriverInfo,
		AppState:                  sparkApp.Status.AppState,
		ExecutionAttempts:         sparkApp.Status.ExecutionAttempts,
		SubmissionAttempts:        sparkApp.Status.SubmissionAttempts,
	}

}

func NewGatewayApplicationMeta(smAppMeta *SparkManagerApplicationMeta, cluster string) *GatewayApplicationMeta {
	return &GatewayApplicationMeta{
		SparkManagerApplicationMeta: *smAppMeta,
		Cluster:                     cluster,
	}
}
