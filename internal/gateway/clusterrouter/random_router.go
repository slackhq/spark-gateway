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
	"math/rand"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/repository"
)

// RandomClusterRouter picks a SparkCluster randomly from it's configured SparkClusters.
type RandomClusterRouter struct {
	clusterRepository repository.ClusterRepository
}

func NewRandomClusterRouter(repo repository.ClusterRepository) ClusterRouter {
	return &RandomClusterRouter{clusterRepository: repo}
}

func (r *RandomClusterRouter) GetCluster(ctx context.Context, namespace string) (*domain.KubeCluster, error) {
	clusters := r.clusterRepository.GetAllWithNamespace(namespace)
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no clusters with namespace %s returned", namespace)
	}
	return &clusters[rand.Intn(len(clusters))], nil
}
