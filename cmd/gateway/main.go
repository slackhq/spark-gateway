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

	cfg "github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/util"

	"github.com/slackhq/spark-gateway/internal/gateway/server"
)

var (
	serviceAuthFlag              = "service-auth-conf"
	confFile                     = flag.String("conf", "configs/config.yaml", "path to config file")
	sparkManagerHostnameFlag     = "spark-manager-hostname-template"
	sparkManagerHostnameTemplate = flag.String(sparkManagerHostnameFlag, "localhost",
		"Defines the template for the SparkManager service name. The Gateway server uses this template to route "+
			"traffic to the specific SparkManager services for various Kubernetes clusters. The Gateway server will replace "+
			"{{.clusterName}} with the name of the Kubernetes cluster where the SparkApplication needs to be submitted or "+
			"from which its status needs to be retrieved.")
)
var sgConfig cfg.SparkGatewayConfig

func init() {
	flag.Usage = func() {
		fmt.Println(flag.CommandLine.FlagUsages())
		os.Exit(0)
	}
	flag.Parse()

	// Require and validate Hostname Template
	if *sparkManagerHostnameTemplate == "" {
		klog.Errorf("--%s is a required flag.", sparkManagerHostnameFlag)
		fmt.Println(flag.CommandLine.FlagUsages())
		os.Exit(1)
	}

	// Validate Hostname Template
	_, err := util.RenderTemplate(*sparkManagerHostnameTemplate, map[string]string{"clusterName": "test-spark-cluster"})
	if err != nil {
		klog.Errorf("template specifed in --%s flag is not valid. Error: %v", sparkManagerHostnameFlag, err)
		os.Exit(1)
	}

	// Read gateway config file and validate
	err = cfg.ConfigUnmarshal(*confFile, &sgConfig)
	if err != nil {
		klog.Errorf("unable to read and unmarshal GatewayConfig from %s path. Error: %v", *confFile, err)
		os.Exit(1)
	}

	errors := sgConfig.Validate()
	if len(errors) > 0 {
		klog.Errorf("'spark-gateway' config has invalid values:\n%s", strings.Join(errors, "\n"))
		os.Exit(1)
	}

}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	ctx := util.SetupSignalHandler()

	if sgConfig.Mode != "local" {
		gin.SetMode(gin.ReleaseMode)
	}

	server, err := server.NewGateway(ctx, &sgConfig, *sparkManagerHostnameTemplate)
	if err != nil {
		klog.Errorf("unable to create gateway server. Error: %v", err)
		os.Exit(1)
	}
	server.Run()
}
