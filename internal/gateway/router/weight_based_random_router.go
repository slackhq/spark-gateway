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

package router

import (
	"context"
	"fmt"
	"github.com/slackhq/spark-gateway/internal/gateway/cluster"
	cfgPkg "github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/model"
	"math/rand"
)

type WeightBasedRandomRouter struct {
	clusterRepository   cluster.ClusterRepository
	clusterRouterConfig cfgPkg.ClusterRouter
}

func NewWeightBasedRandomRouter(
	clusterRepository cluster.ClusterRepository,
	clusterRouterConfig cfgPkg.ClusterRouter,
) ClusterRouter {
	return WeightBasedRandomRouter{
		clusterRepository:   clusterRepository,
		clusterRouterConfig: clusterRouterConfig,
	}
}

// GetCluster returns a Kubernetes cluster where the SparkApplication should be submitted. It uses the
// clusterRouterConfig and per-dimension metrics like cluster or cluster-namespace metrics to determine the best
// cluster to submit the new SparkApplication to. GetCluster uses GetClusterMetricFamilies method to read
// metrics in Prometheus text-based exposition format.
/*
	GetCluster Overview:

	Configs:
    clusterRouter.type: weightBasedRandom
	clusterRouter.dimension: cluster

	# Submitted SparkApp namespace: application.ObjectMeta.Namespace
	namespace = ns1

	# Determine list of clusters that have ns1
	validClusters = [cluster A, cluster C] # assuming these clusters have ns1 configured in clusters config


	# Read user defined routingWeights
	cluster A weight = weight(clusterA)
	cluster B weight = weight(clusterB)
	total weight = weight(clusterA) + weight(clusterB)

	# Generate a random number between 0 and total weight
	randomInt := rand.Intn(int(*totalWeight))
	weightCount := 0
	for cluster, weight := range weightsMap {
		if randomInt > weightCount && randomInt <= (weightCount+weight) {
			chosenCluster = cluster
			break
		}
		weightCount += weight
	}

	return chosenCluster
*/
func (r WeightBasedRandomRouter) GetCluster(ctx context.Context, namespace string) (*model.KubeCluster, error) {

	clustersList, err := r.clusterRepository.GetAllWithNamespace(namespace)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error listing clusters from clusterRepository: %w", err))
	}

	switch len(clustersList) {
	case 0:
		return nil, gatewayerrors.NewFrom(fmt.Errorf("no clusters found with namespace %s in Gateway configs", namespace))
	case 1:
		return &clustersList[0], nil
	}

	// clusterName to metric map
	weightsMap := make(map[string]*metric)
	weightsMap, totalWeight := ReadWeightConfigs(weightsMap, clustersList, r.clusterRouterConfig, namespace)
	if len(weightsMap) == 0 {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("unable to find any suitable cluster for routing"))
	}

	randomInt := rand.Intn(int(*totalWeight))
	chosenClusterId := chooseClusterIDRandom(weightsMap, randomInt)

	chosenCluster, err := r.clusterRepository.GetById(chosenClusterId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting a cluster with chosen cluster ID. %w", err))
	}
	return chosenCluster, nil
}

func chooseClusterIDRandom(weightsMap map[string]*metric, randomInt int) string {
	var chosenClusterId string
	clusterIDs := GetOrderedKeys(weightsMap)
	weightCount := 0
	for _, id := range clusterIDs {
		if randomInt >= weightCount && randomInt < (weightCount+int(weightsMap[id].Weight)) {
			chosenClusterId = id
			break
		}
		weightCount += int(weightsMap[id].Weight)
	}
	return chosenClusterId
}
