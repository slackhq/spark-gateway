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

package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/slackhq/spark-gateway/internal/gateway/middleware"
	"github.com/slackhq/spark-gateway/pkg/model"
	"github.com/slackhq/spark-gateway/pkg/util"
)

const (
	KubeConfigAuthType     = "kubeconfig"
	ServiceAccountAuthType = "serviceaccount"
)

// Valid values
var validClusterAuthTypes []string = []string{KubeConfigAuthType, ServiceAccountAuthType}

type ClusterRouterType string

var RandomRouter ClusterRouterType = "random"
var WeightBasedRouter ClusterRouterType = "weightBased"
var WeightBasedRandomRouter ClusterRouterType = "weightBasedRandom"

var validClusterRouterTypes = []ClusterRouterType{
	RandomRouter,
	WeightBasedRouter,
	WeightBasedRandomRouter,
}

type ClusterRouterDimensionType string

var NamespaceDimension ClusterRouterDimensionType = "namespace"
var ClusterDimension ClusterRouterDimensionType = "cluster"

var validClusterRouterDimensionTypes = []ClusterRouterDimensionType{
	NamespaceDimension,
	ClusterDimension,
}

type PrometheusQuery struct {
	Metric           string            `koanf:"metric"`
	AdditionalLabels map[string]string `koanf:"additionalLabels"`
}

type ClusterRouter struct {
	Type            ClusterRouterType          `koanf:"type"`
	FallbackType    ClusterRouterType          `koanf:"fallbackType"`
	Dimension       ClusterRouterDimensionType `koanf:"dimension"`
	PrometheusQuery PrometheusQuery            `koanf:"prometheusQuery"`
}

type UnmarshalableConfig interface {
	Unmarshal(k *koanf.Koanf) error
	Key() string
}

type BaseConfig struct{}

func (b *BaseConfig) Unmarshal(k *koanf.Koanf) error {
	if err := k.Unmarshal(b.Key(), b); err != nil {
		return fmt.Errorf("error unmarshaling config [%s]: %w", b.Key(), err)
	}

	return nil
}

func (b *BaseConfig) Key() string {
	return "base"
}

func ConfigUnmarshal(path string, conf UnmarshalableConfig) error {
	k := koanf.New(".")

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return fmt.Errorf("error parsing config file: %s", err)
	}

	if err := conf.Unmarshal(k); err != nil {
		return fmt.Errorf("could not unmarshal config: %w", err)
	}

	return nil
}

type BasicAllowAuthServiceConfig struct {
	Allow []string `koanf:"allow"`
	Deny  []string `koanf:"deny"`
}

type Database struct {
	Enable       bool   `koanf:"enable"`
	DatabaseName string `koanf:"databaseName"`
	Hostname     string `koanf:"hostname"`
	Port         string `koanf:"port"`
	Username     string `koanf:"username"`
	Password     string `koanf:"password"`
}

type MiddlewareDefinition struct {
	Type string                       `koanf:"type"`
	Conf middleware.MiddlewareConfMap `koanf:"conf"`
}

type GatewayConfig struct {
	*BaseConfig
	GatewayApiVersion  string                   `koanf:"gatewayApiVersion"`
	GatewayPort        string                   `koanf:"gatewayPort"`
	Middleware         []MiddlewareDefinition   `koanf:"middleware"`
	StatusUrlTemplates model.StatusUrlTemplates `koanf:"statusUrlTemplates"`
	EnableSwaggerUI    bool                     `koanf:"enableSwaggerUI"`
}

func (g *GatewayConfig) Key() string {
	return "gateway"
}

type MetricsServer struct {
	Endpoint string `koanf:"endpoint"`
	Port     string `koanf:"port"`
}

type SparkManagerConfig struct {
	*BaseConfig
	ClusterAuthType string        `koanf:"clusterAuthType"`
	Database        Database      `koanf:"database"`
	MetricsServer   MetricsServer `koanf:"metricsServer"`
}

func (sm *SparkManagerConfig) Key() string {
	return "sparkManager"
}

func (c *SparkManagerConfig) Validate() (errorMessages []string) {
	// ClusterAuthType
	if !util.ValueExists(c.ClusterAuthType, validClusterAuthTypes) {
		errorMessages = append(errorMessages, fmt.Sprintf("config error: invalid 'sparkManager.clusterAuthType' '%s', valid clusterAuthType values: %s", c.ClusterAuthType, strings.Join(validClusterAuthTypes, ", ")))
	}

	if c.Database.Enable {
		if c.Database.Password == "" {
			c.Database.Password = os.Getenv("DB_PASSWORD")
		}
		// Database hostname and port should exist
		if c.Database.Hostname == "" || c.Database.Port == "" {
			errorMessages = append(errorMessages, "config error: 'sparkManager.database.hostname' and 'sparkManager.database.port' must be specified")
		}

		// Database Username or Password must be set
		if c.Database.Username == "" {
			errorMessages = append(errorMessages, "config error: 'sparkManager.database.username' config must be specified")
		}

		if c.Database.Password == "" {
			errorMessages = append(errorMessages, "config error: 'sparkManager.database.password' config or DB_PASSWORD environment variable must be specified")
		}

	}

	return errorMessages
}

type DebugPort struct {
	SparkManagerPort string `koanf:"sparkManagerPort"`
	MetricsPort      string `koanf:"metricsPort"`
}

type SparkGatewayConfig struct {
	*BaseConfig
	KubeClusters       []model.KubeCluster  `koanf:"clusters"`
	ClusterRouter      ClusterRouter        `koanf:"clusterRouter"`
	DefaultLogLines    int                  `koanf:"defaultLogLines"`
	Mode               string               `koanf:"mode"`
	SelectorKey        string               `koanf:"selectorKey"`
	SelectorValue      string               `koanf:"selectorValue"`
	SparkManagerPort   string               `koanf:"sparkManagerPort"`
	GatewayConfig      GatewayConfig        `koanf:"gateway"`
	SparkManagerConfig SparkManagerConfig   `koanf:"sparkManager"`
	DebugPorts         map[string]DebugPort `koanf:"debugPorts"`
}

func (c *SparkGatewayConfig) Unmarshal(k *koanf.Koanf) error {
	if err := k.Unmarshal(c.Key(), &c); err != nil {
		return fmt.Errorf("error unmarshaling GatewayConfig: %w", err)
	}

	return nil
}

// SparkGatewayConfig maps to the whole config file, so the key should be ""
func (c *SparkGatewayConfig) Key() string {
	return ""
}

func (c *SparkGatewayConfig) Validate() (errorMessages []string) {

	// Set defaults
	c.ConfigDefaulter()

	if len(c.KubeClusters) == 0 {
		errorMessages = append(errorMessages, "config error: 'kubeClusters' config must be specified")
	}

	// KubeClusters
	seenClusterIds := map[string]bool{}
	for _, cluster := range c.KubeClusters {
		// Check if dupe id exists
		_, ok := seenClusterIds[cluster.ClusterId]
		if ok {
			errorMessages = append(errorMessages, fmt.Sprintf("duplicate cluster id found in clusters configuration: '%s'", cluster.ClusterId))
		}

		seenClusterIds[cluster.ClusterId] = true

		errs := model.ValidateCluster(cluster)
		errorMessages = append(errorMessages, errs...)
	}

	if !util.ValueExists(c.ClusterRouter.Type, validClusterRouterTypes) {
		errorMessages = append(errorMessages, fmt.Sprintf("config error: invalid 'clusterRouter.type' '%s', valid values: %v", c.ClusterRouter.Type, validClusterRouterTypes))
	}

	if !util.ValueExists(c.ClusterRouter.FallbackType, validClusterRouterTypes) {
		errorMessages = append(errorMessages, fmt.Sprintf("config error: invalid 'clusterRouter.fallbackType' '%s', valid values: %v", c.ClusterRouter.Type, validClusterRouterTypes))
	}

	if !util.ValueExists(c.ClusterRouter.Dimension, validClusterRouterDimensionTypes) {
		errorMessages = append(errorMessages, fmt.Sprintf("config error: invalid 'clusterRouter.dimension' '%s', valid values: %v", c.ClusterRouter.Dimension, validClusterRouterDimensionTypes))
	}

	return errorMessages
}

func (c *SparkGatewayConfig) ConfigDefaulter() {
	c.KubeClustersDefaulter()
	c.ClusterRouterDefaulter()
}

func (c *SparkGatewayConfig) KubeClustersDefaulter() {
	// set default routingWeight to 1
	for _, c := range c.KubeClusters {
		if c.RoutingWeight == float64(0) {
			c.RoutingWeight = 1.0
		}

		for _, ns := range c.Namespaces {
			if ns.RoutingWeight == float64(0) {
				ns.RoutingWeight = 1.0
			}
		}
	}
}

func (c *SparkGatewayConfig) GetKubeCluster(clusterName string) *model.KubeCluster {
	for _, cluster := range c.KubeClusters {
		if cluster.Name == clusterName {
			return &cluster
		}
	}
	return nil
}

func (c *SparkGatewayConfig) ClusterRouterDefaulter() {
	if c.ClusterRouter.Type == "" {
		c.ClusterRouter.Type = WeightBasedRandomRouter
	}
	if c.ClusterRouter.FallbackType == "" {
		c.ClusterRouter.FallbackType = WeightBasedRandomRouter
	}
}
