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
	"fmt"

	"github.com/slackhq/spark-gateway/internal/domain"
	"k8s.io/klog/v2"
)

//go:generate moq -rm  -out mockclusterrepository.go . ClusterRepository

type ClusterRepository interface {
	GetByName(cluster string) (*domain.KubeCluster, error)
	GetById(clusterId string) (*domain.KubeCluster, error)
	GetAll() ([]domain.KubeCluster, error)
	GetAllWithNamespace(namespace string) ([]domain.KubeCluster, error)
}

type LocalClusterRepo struct {
	KubeClusters map[string]domain.KubeCluster
}

func NewLocalClusterRepo(clusters []domain.KubeCluster) (*LocalClusterRepo, error) {

	if len(clusters) == 0 {
		return nil, fmt.Errorf("NewLocalClusterRepo: No clusters passed")
	}

	clustersById := map[string]domain.KubeCluster{}

	for _, cluster := range clusters {
		clustersById[cluster.ClusterId] = cluster
	}

	return &LocalClusterRepo{KubeClusters: clustersById}, nil
}

func (r *LocalClusterRepo) GetByName(cluster string) (*domain.KubeCluster, error) {
	for _, kubeCluster := range r.KubeClusters {
		if kubeCluster.Name == cluster {
			return &kubeCluster, nil
		}
	}

	return nil, fmt.Errorf("cluster does not exist: %s", cluster)
}

func (r *LocalClusterRepo) GetById(clusterId string) (*domain.KubeCluster, error) {

	cluster, ok := r.KubeClusters[clusterId]

	if !ok {
		return nil, fmt.Errorf("cluster does not exist: %s", clusterId)
	}

	return &cluster, nil
}

func (r *LocalClusterRepo) GetAll() ([]domain.KubeCluster, error) {
	var clusters []domain.KubeCluster

	for _, cluster := range r.KubeClusters {
		clusters = append(clusters, cluster)
	}

	if len(clusters) == 0 {
		klog.Warningf("GetAll: no clusters found in clusters config")
	}

	return clusters, nil
}

func (r *LocalClusterRepo) GetAllWithNamespace(namespace string) ([]domain.KubeCluster, error) {
	var clusters []domain.KubeCluster

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
