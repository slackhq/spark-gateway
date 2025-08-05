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

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/sparkManager"
	cfg "github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/util"
)

var (
	confFile = flag.String("conf", "configs/config.yaml", "path to config file")
	cluster  = flag.String("cluster", "", "Kubernetes Cluster Endpoint")
)
var sgConfig cfg.SparkGatewayConfig

func init() {
	flag.Usage = func() {
		fmt.Println(flag.CommandLine.FlagUsages())
		os.Exit(0)
	}
	flag.Parse()

	// Read gateway config file and validate
	err := cfg.ConfigUnmarshal(*confFile, &sgConfig)
	if err != nil {
		klog.Errorf("unable to read and unmarshal GatewayConfig from %s path. Error: %v", *confFile, err)
		os.Exit(1)
	}

	errors := sgConfig.Validate()
	if len(errors) > 0 {
		klog.Errorf("'spark-gateway' config has invalid values:\n%s", strings.Join(errors, "\n"))
		os.Exit(1)
	}

	errors = sgConfig.SparkManagerConfig.Validate()
	if len(errors) > 0 {
		klog.Errorf("'spark-gateway.sparkManager' config has invalid values:\n%s", strings.Join(errors, "\n"))
		os.Exit(1)
	}

}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	ctx := util.SetupSignalHandler()
	klog.Infof("staring sparkManager server for %s kube cluster\n", *cluster)

	if sgConfig.Mode != "debug" {
		gin.SetMode(gin.ReleaseMode)
	}

	if *cluster == "" {
		klog.Fatal("--cluster flag must be set to Kube cluster name")
		os.Exit(1)
	}
	klog.Infof("staring sparkManager server for %s kube cluster\n", *cluster)

	// set metrics Port
	if debugPort, ok := sgConfig.DebugPorts[*cluster]; ok {
		klog.Infof("debugPorts config found for %s cluster\n", *cluster)
		sgConfig.SparkManagerPort = debugPort.SparkManagerPort
		sgConfig.SparkManagerConfig.MetricsServer.Port = debugPort.MetricsPort
	}

	server, err := sparkManager.NewSparkManager(ctx, &sgConfig, *cluster)
	if err != nil {
		klog.Fatalf("unable to create SparkManager server. Error: %v", err)
		os.Exit(1)
	}
	server.Run()
}
