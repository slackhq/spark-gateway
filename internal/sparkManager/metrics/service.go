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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/slackhq/spark-gateway/internal/domain"
	"k8s.io/klog/v2"
)

type service struct {
	kubeCluster *domain.KubeCluster
	repository  *Repository
}

func NewService(repository *Repository, kubeCluster *domain.KubeCluster) Service {
	m := &service{
		kubeCluster: kubeCluster,
		repository:  repository,
	}

	return m
}

func (s *service) RecordMetrics(done chan bool, ticker *time.Ticker, metrics Metrics) {
	klog.Info("recordMetrics: start recording metrics")
	// Collect metrics during initialization. Ticker will tick after set duration.
	s.setClusterMetrics(metrics)
	s.setNamespaceMetrics(metrics)

	for {
		select {
		case <-ticker.C:
			klog.Info("recordMetrics: Collecting metrics")
			s.setClusterMetrics(metrics)
			s.setNamespaceMetrics(metrics)
		case <-done:
			klog.Info("recordMetrics: goroutine done")
			return
		}
	}

}

func (s *service) setClusterMetrics(metrics Metrics) {

	sparkApplicationList := s.repository.GetFilteredSparkApplicationsByCluster()

	countByCluster := len(sparkApplicationList)
	metrics.sparkApplicationCount.With(prometheus.Labels{"cluster": s.kubeCluster.Name, "namespace": ""}).Set(float64(countByCluster))

	cpuByCluster := s.repository.GetTotalCPUAllocation(sparkApplicationList)
	metrics.cpuAllocated.With(prometheus.Labels{"cluster": s.kubeCluster.Name, "namespace": ""}).Set(cpuByCluster)
}

func (s *service) setNamespaceMetrics(metrics Metrics) {
	// SparkApplicationCountByNamespace
	for _, ns := range s.kubeCluster.Namespaces {
		sparkApplicationList := s.repository.GetFilteredSparkApplicationsByNamespace(ns.Name)

		countByNamespace := len(sparkApplicationList)
		metrics.sparkApplicationCount.With(prometheus.Labels{"cluster": s.kubeCluster.Name, "namespace": ns.Name}).Set(float64(countByNamespace))

		cpuByNamespace := s.repository.GetTotalCPUAllocation(sparkApplicationList)
		metrics.cpuAllocated.With(prometheus.Labels{"cluster": s.kubeCluster.Name, "namespace": ns.Name}).Set(cpuByNamespace)
	}
}
