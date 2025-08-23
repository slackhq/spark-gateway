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
	"math"
	"net/http"
	"strings"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/repository"
	cfgPkg "github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	sharedHttp "github.com/slackhq/spark-gateway/internal/shared/http"
	"github.com/slackhq/spark-gateway/internal/shared/util"
)

const (
	clusterLabelKey   = "cluster"
	namespaceLabelKey = "namespace"
)

type metric struct {
	Weight      float64
	WeightRatio float64
	Metric      float64
	MetricRatio float64
	RatioDiff   float64
}

type WeightBasedRouter struct {
	clusterRepository            repository.ClusterRepository
	clusterRouterConfig          cfgPkg.ClusterRouter
	sparkManagerHostnameTemplate string
	metricsServerConfig          cfgPkg.MetricsServer
	debugPorts                   map[string]cfgPkg.DebugPort
}

func NewWeightBasedRouter(
	clusterRepository repository.ClusterRepository,
	clusterRouterConfig cfgPkg.ClusterRouter,
	sparkManagerHostnameTemplate string,
	metricsServerConfig cfgPkg.MetricsServer,
	debugPorts map[string]cfgPkg.DebugPort,
) ClusterRouter {
	return &WeightBasedRouter{
		clusterRepository:            clusterRepository,
		clusterRouterConfig:          clusterRouterConfig,
		sparkManagerHostnameTemplate: sparkManagerHostnameTemplate,
		metricsServerConfig:          metricsServerConfig,
		debugPorts:                   debugPorts,
	}
}

// GetCluster returns a Kubernetes cluster where the SparkApplication should be submitted. It uses the
// clusterRouterConfig and per-dimension metrics like cluster or cluster-namespace metrics to determine the best
// cluster to submit the new SparkApplication to. GetCluster uses GetClusterMetricFamilies method to read
// metrics in Prometheus text-based exposition format.
/*
	GetCluster Overview:

	Configs:
    clusterRouter.type: weightBased
	clusterRouter.dimension: cluster
	clusterRouter.prometheusQuery.metric: spark_application_count

	# Submitted SparkApp namespace: application.ObjectMeta.Namespace
	namespace = ns1

	# Determine list of clusters that have ns1
	validClusters = [cluster A, cluster C] # assuming these clusters have ns1 configured in clusters config

	# Determine weight ratios based on user defined routingWeights
	cluster A = weight(clusterA) / (weight(clusterA) + weight(clusterC))
	cluster A weight ratio = 15 / (15 + 10) = 0.6
	cluster C weight ratio = 10 / (15 + 10) = 0.4

	# Determine current ratios based on spark_application_count metric
	total count = spark_application_count(clusterA) + spark_application_count(clusterC) = 35 + 30 = 65
	cluster A current ratio = spark_application_count(clusterA) / total count = 35 / 65 = 0.54
	cluster C current ratio = spark_application_count(clusterC) / total count = 30 / 65 = 0.46

	# Determine the difference in ratios
	cluster A difference = cluster A weight ratio - cluster A current ratio  = 0.6 - 0.54 =  0.04
	cluster C difference = cluster C weight ratio - cluster C current ratio  = 0.4 - 0.46 = -0.06

	# Choose a cluster with the highest difference
	chosen_cluster = max_difference(cluster A difference, cluster C difference) = max(0.04, -0.06) = cluster A
	return cluster A
*/
func (r *WeightBasedRouter) GetCluster(ctx context.Context, namespace string) (*domain.KubeCluster, error) {

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
	metricsMap := make(map[string]*metric)
	metricsMap, _ = ReadWeightConfigs(metricsMap, clustersList, r.clusterRouterConfig, namespace)
	if len(metricsMap) == 0 {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("unable to find any suitable cluster for routing"))
	}

	// Fetch metrics for relevant clusters
	totalMetric := float64(0)
	for _, c := range clustersList {

		// set metrics server port
		metricsPort := r.metricsServerConfig.Port
		if port, ok := r.debugPorts[c.Name]; ok {
			metricsPort = port.MetricsPort
		}

		metricFamilies, err := GetClusterMetricFamilies(ctx, c, r.sparkManagerHostnameTemplate, metricsPort, r.metricsServerConfig.Endpoint)
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting metrics from SparkManager %s: %w", c.ClusterId, err))
		}

		metricFamily, ok := metricFamilies[r.clusterRouterConfig.PrometheusQuery.Metric]
		if !ok {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("could not find metric %s", r.clusterRouterConfig.PrometheusQuery.Metric))
		}

		targetLabels := GetTargetLabels(r.clusterRouterConfig, c.Name, namespace)
		targetMetrics := GetTargetMetrics(metricFamily.GetMetric(), targetLabels)
		switch tsLength := len(targetMetrics); {
		case tsLength == 0:
			return nil, gatewayerrors.NewFrom(fmt.Errorf("no timeseries exist with target labels: %v", targetMetrics))
		case tsLength > 1:
			return nil, gatewayerrors.NewFrom(fmt.Errorf("more than 1 timeseries exist with target labels: %v", targetMetrics))
		}

		metricVal := targetMetrics[0].Gauge.GetValue()

		metricsMap[c.ClusterId].Metric = metricVal
		totalMetric += metricVal
	}

	chosenClusterId := chooseClusterID(metricsMap, totalMetric)
	chosenCluster, err := r.clusterRepository.GetById(chosenClusterId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting a cluster with chose cluster ID. %w", err))
	}
	return chosenCluster, nil
}

// chooseCluster will determine a cluster's metric ratio, then determine the difference between a cluster's weight
// ratio and its metric ratio, and choose cluster with the max ratio difference. If totalMetric is 0, the cluster with
// the highest weight will be chosen. If all clusters have the same weight, the first cluster in the clusters list will
// be chosen.
func chooseClusterID(metricsMap map[string]*metric, totalMetric float64) string {
	maxDiff := -math.MaxFloat64
	var chosenClusterId string
	clusterIDs := GetOrderedKeys(metricsMap)

	for _, clusterID := range clusterIDs {
		m := metricsMap[clusterID]
		if totalMetric > 0 {
			m.MetricRatio = m.Metric / totalMetric
		} else {
			m.MetricRatio = 1
		}
		m.RatioDiff = m.WeightRatio - m.MetricRatio
		if m.RatioDiff > maxDiff {
			maxDiff = m.RatioDiff
			chosenClusterId = clusterID
		}
	}

	return chosenClusterId
}

func ReadWeightConfigs(metricsMap map[string]*metric, clusters []domain.KubeCluster, routerConfig cfgPkg.ClusterRouter, namespace string) (map[string]*metric, *float64) {
	// Determine routing weights for relevant clusters and namespaces
	// Include:
	// - cluster dimension: cluster routingWeights for clusters that contain SparkApp's namespace
	// - namespace dimension: namespace routingWeights for SparkApp's namespace in clusters

	totalWeight := float64(0)
	for _, c := range clusters {
		var m metric
		if routerConfig.Dimension == cfgPkg.ClusterDimension {
			m.Weight = c.RoutingWeight
		} else if routerConfig.Dimension == cfgPkg.NamespaceDimension {
			ns, err := c.GetNamespaceByName(namespace)
			if err != nil {
				klog.Info(err)
				continue
			}
			m.Weight = ns.RoutingWeight
		}

		metricsMap[c.ClusterId] = &m
		totalWeight += m.Weight
	}

	// Add weight ratios to weightsMap
	for _, m := range metricsMap {
		r := m.Weight / totalWeight
		m.WeightRatio = r
	}
	return metricsMap, &totalWeight
}

// Identify targetLabels for metrics
func GetTargetLabels(clusterRouterConfig cfgPkg.ClusterRouter, clusterName string, namespace string) map[string]string {
	var targetLabels map[string]string

	switch clusterRouterConfig.Dimension {
	case cfgPkg.ClusterDimension:
		targetLabels = map[string]string{
			clusterLabelKey:   clusterName,
			namespaceLabelKey: "",
		}
	case cfgPkg.NamespaceDimension:
		targetLabels = map[string]string{
			clusterLabelKey:   clusterName,
			namespaceLabelKey: namespace,
		}
	}

	// Add additionalLabels to targetLabels
	targetLabels = util.MergeMaps(targetLabels, clusterRouterConfig.PrometheusQuery.AdditionalLabels)

	return targetLabels
}

func GetClusterMetricFamilies(
	ctx context.Context,
	c domain.KubeCluster,
	sparkManagerHostnameTemplate string,
	metricsServerPort,
	metricsServerEndpoint string,
) (map[string]*io_prometheus_client.MetricFamily, error) {
	hostname, err := util.RenderTemplate(sparkManagerHostnameTemplate, map[string]string{"clusterName": c.Name})
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	metricsUrl := fmt.Sprintf("http://%s:%s%s", *hostname, metricsServerPort, metricsServerEndpoint)

	request, err := http.NewRequest(http.MethodGet, metricsUrl, nil)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating %s request: %w", "GET", err))
	}

	resp, respBody, err := sharedHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	err = sharedHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}

	responseString := string(*respBody)

	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(strings.NewReader(responseString))
	if err != nil {
		return nil, gatewayerrors.NewFrom(err)
	}
	return mf, nil
}

func GetTargetMetrics(metrics []*io_prometheus_client.Metric, targetLabels map[string]string) []*io_prometheus_client.Metric {
	var matchingMetrics []*io_prometheus_client.Metric
	for _, metric := range metrics {
		if ContainsLabels(metric.Label, targetLabels) {
			matchingMetrics = append(matchingMetrics, metric)
		}
	}
	return matchingMetrics
}

func ContainsLabels(labels []*io_prometheus_client.LabelPair, targetLabels map[string]string) bool {
	labelMap := GenerateMap(labels)
	match := true
	for label, value := range targetLabels {
		if labelMap[label] != value {
			match = false
			break
		}
	}
	return match
}

func GenerateMap(labels []*io_prometheus_client.LabelPair) map[string]string {
	var labelMap = map[string]string{}
	for _, label := range labels {
		labelMap[*label.Name] = *label.Value
	}
	return labelMap
}
