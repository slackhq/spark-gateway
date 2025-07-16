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

	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/tests"
	"github.com/slackhq/spark-gateway/pkg/util"
)

var (
	gatewayUrlFlag     = "gateway-url"
	helmTestGatewayUrl = flag.String(gatewayUrlFlag, "", "Service name to use for Helm tests")
)

func init() {
	flag.Usage = func() {
		fmt.Println(flag.CommandLine.FlagUsages())
		os.Exit(0)
	}
	flag.Parse()
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	ctx := util.SetupSignalHandler()

	if *helmTestGatewayUrl == "" {
		klog.Errorf("%s flag must be set for helm tests", *helmTestGatewayUrl)
		os.Exit(1)
	}

	tests.Run(ctx, *helmTestGatewayUrl)

}
