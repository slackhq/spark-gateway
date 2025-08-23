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

package clusterrouter

import (
	"context"
	"fmt"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/repository"
	cfg "github.com/slackhq/spark-gateway/internal/shared/config"
)

//go:generate moq -rm  -out mockclusterrouter.go . ClusterRouter
type ClusterRouter interface {
	GetCluster(ctx context.Context, namespace string) (*domain.KubeCluster, error)
}

func GetClusterRouter(
	routerType cfg.ClusterRouterType,
	localClusterRepo repository.ClusterRepository,
	clusterRouterConfig cfg.ClusterRouter,
	sparkManagerHostnameTemplate string,
	metricsServerConfig cfg.MetricsServer,
	debugPorts map[string]cfg.DebugPort) (ClusterRouter, error) {

	var clusterRouter ClusterRouter
	switch routerType {
	case cfg.RandomRouter:
		clusterRouter = NewRandomClusterRouter(localClusterRepo)
	case cfg.WeightBasedRouter:
		clusterRouter = NewWeightBasedRouter(
			localClusterRepo,
			clusterRouterConfig,
			sparkManagerHostnameTemplate,
			metricsServerConfig,
			debugPorts,
		)
	case cfg.WeightBasedRandomRouter:
		clusterRouter = NewWeightBasedRandomRouter(
			localClusterRepo,
			clusterRouterConfig,
		)
	default:
		return nil, fmt.Errorf("unknown cluster router type: %s", routerType)
	}
	return clusterRouter, nil
}
