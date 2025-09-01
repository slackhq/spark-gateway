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

package cluster

import (
	"fmt"

	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/pkg/model"
)

//go:generate moq -rm  -out mockclusterrepository.go . ClusterRepository

type ClusterRepository interface {
	GetByName(cluster string) (*model.KubeCluster, error)
	GetById(clusterId string) (*model.KubeCluster, error)
	GetAll() ([]model.KubeCluster, error)
	GetAllWithNamespace(namespace string) ([]model.KubeCluster, error)
}

type LocalClusterRepo struct {
	KubeClusters map[string]model.KubeCluster
}

func NewLocalClusterRepo(clusters []model.KubeCluster) (*LocalClusterRepo, error) {

	if len(clusters) == 0 {
		return nil, fmt.Errorf("NewLocalClusterRepo: No clusters passed")
	}

	clustersById := map[string]model.KubeCluster{}

	for _, cluster := range clusters {
		clustersById[cluster.ClusterId] = cluster
	}

	return &LocalClusterRepo{KubeClusters: clustersById}, nil
}

func (r *LocalClusterRepo) GetByName(cluster string) (*model.KubeCluster, error) {
	for _, kubeCluster := range r.KubeClusters {
		if kubeCluster.Name == cluster {
			return &kubeCluster, nil
		}
	}

	return nil, fmt.Errorf("cluster does not exist: %s", cluster)
}

func (r *LocalClusterRepo) GetById(clusterId string) (*model.KubeCluster, error) {

	cluster, ok := r.KubeClusters[clusterId]

	if !ok {
		return nil, fmt.Errorf("cluster does not exist: %s", clusterId)
	}

	return &cluster, nil
}

func (r *LocalClusterRepo) GetAll() ([]model.KubeCluster, error) {
	var clusters []model.KubeCluster

	for _, cluster := range r.KubeClusters {
		clusters = append(clusters, cluster)
	}

	if len(clusters) == 0 {
		return nil, fmt.Errorf("GetAll: no clusters found in clusters config")
	}

	return clusters, nil
}

func (r *LocalClusterRepo) GetAllWithNamespace(namespace string) ([]model.KubeCluster, error) {
	var clusters []model.KubeCluster

	allClusters, err := r.GetAll()
	if err != nil {
		return nil, err
	}

	for _, cluster := range allClusters {
		ns, err := cluster.GetNamespaceByName(namespace)
		if err != nil {
			klog.Error(fmt.Errorf("error getting namespace by name: %w", err))
		}
		if ns != nil {
			clusters = append(clusters, cluster)
		}
	}

	if len(clusters) == 0 {
		klog.Warningf("GetAllWithNamespace: No clusters found in clusters config with namespace: %s", namespace)
	}

	return clusters, nil
}
