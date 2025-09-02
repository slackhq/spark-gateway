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

package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/domain"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
)

func Run(ctx context.Context, gatewayHostname string) {

	sparkApplication := getTestSparkApp()

	klog.Infof("Hostname: %s\n", gatewayHostname)
	klog.Infof("SparkApplication Name: %s/%s\n", sparkApplication.Namespace, sparkApplication.Name)

	// Create
	gatewayId := TestCreateSparkApplication(ctx, sparkApplication, gatewayHostname)

	// Get
	TestGetSparkApplication(ctx, sparkApplication, gatewayHostname, gatewayId)

	// Get Status
	// Status will be nil given that we don't have a Spark Operator running in the Spark-Gateway chart
	TestGetSparkApplicationStatus(ctx, sparkApplication, gatewayHostname, gatewayId)

	// Get logs
	// Logs will be empty given that we don't have a Spark Operator running in the Spark-Gateway chart
	TestGetSparkApplicationLogs(ctx, sparkApplication, gatewayHostname, gatewayId)

	// Delete
	TestDeleteSparkApplication(ctx, sparkApplication, gatewayHostname, gatewayId)

	fmt.Printf("All Tests Succeeded.")
}

func TestCreateSparkApplication(ctx context.Context, sparkApplication *v1beta2.SparkApplication, hostname string) string {
	body, err := json.Marshal(sparkApplication)
	if err != nil {
		klog.Errorf("failed to marshal SparkApplication: %v", err)
	}

	url := fmt.Sprintf("%s/v2/applications", hostname)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		klog.Error(fmt.Errorf("error creating %s request: %w", http.MethodPost, err))
		os.Exit(1)
	}

	request = AddBasicAuth(request, "admin")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	var sparkApp v1beta2.SparkApplication
	if err = json.Unmarshal(*respBody, &sparkApp); err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	return sparkApp.Name
}

func TestGetSparkApplication(ctx context.Context, sparkApplication *v1beta2.SparkApplication, hostname string, gatewayId string) {

	url := fmt.Sprintf("%s/v2/applications/%s", hostname, gatewayId)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		klog.Error(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
		os.Exit(1)
	}

	request = AddBasicAuth(request, "admin")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	var app domain.GatewayApplication
	if err := json.Unmarshal(*respBody, &app); err != nil {
		klog.Errorf("failed to Unmarshal JSON response: %v", err)
		os.Exit(1)
	}

	// Check a few fields
	if sparkApplication.ObjectMeta.Name != app.SparkApplication.Name ||
		sparkApplication.ObjectMeta.Namespace != app.SparkApplication.Namespace ||
		*sparkApplication.Spec.MainApplicationFile != *app.SparkApplication.Spec.MainApplicationFile ||
		*sparkApplication.Spec.MainClass != *app.SparkApplication.Spec.MainClass {
		klog.Errorf("TestGetSparkApplication failed")
		os.Exit(1)
	}

}

func TestGetSparkApplicationStatus(ctx context.Context, sparkApplication *v1beta2.SparkApplication, hostname string, gatewayId string) {

	url := fmt.Sprintf("%s/v2/applications/%s/status", hostname, gatewayId)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		klog.Error(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
		os.Exit(1)
	}

	request = AddBasicAuth(request, "admin")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

	// Status will be nil given that we dont have a Spark Operator running
	var status v1beta2.SparkApplicationStatus
	if err := json.Unmarshal(*respBody, &status); err != nil {
		klog.Errorf("failed to Unmarshal JSON response: %v", err)
		os.Exit(1)
	}

}

// Logs will be empty given that we don't have a Spark Operator running in the Spark-Gateway chart
func TestGetSparkApplicationLogs(ctx context.Context, sparkApplication *v1beta2.SparkApplication, hostname string, gatewayId string) {

	url := fmt.Sprintf("%s/v2/applications/%s/logs", hostname, gatewayId)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		klog.Error(fmt.Errorf("error creating %s request: %w", http.MethodGet, err))
		os.Exit(1)
	}

	request = AddBasicAuth(request, "admin")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		if err.Error() != "spark driver does not exist, cannot fetch logs" {
			os.Exit(1)
		}
	}

	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}

}

func TestDeleteSparkApplication(ctx context.Context, sparkApplication *v1beta2.SparkApplication, hostname string, gatewayId string) {

	url := fmt.Sprintf("%s/v2/applications/%s", hostname, gatewayId)

	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		klog.Error(fmt.Errorf("error creating %s request: %w", http.MethodDelete, err))
		os.Exit(1)
	}

	request = AddBasicAuth(request, "admin")

	resp, respBody, err := sgHttp.HttpRequest(ctx, &http.Client{}, request)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}
	err = sgHttp.CheckJsonResponse(resp, respBody)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}
}

func AddBasicAuth(request *http.Request, basicAuthUser string) *http.Request {
	auth := basicAuthUser + ":"
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(auth))
	request.Header.Add("Authorization", "Basic "+encodedAuth)
	return request
}

func getTestSparkApp() *v1beta2.SparkApplication {
	labels := map[string]string{}
	appType := v1beta2.SparkApplicationTypeScala
	file := "local:///opt/spark/job.jar"
	args := []string{"-arg1"}
	className := "com.example.SparkJob"
	proxyUser := "test-user"
	jars := []string{"test-jar.jar"}
	driverMemory := "4g"
	driverCores := int32(1)
	executorMemory := "4g"
	executorCores := int32(1)
	initialExecutors := int32(10)
	minExecutors := int32(1)
	maxExecutors := int32(100)
	sparkConf := map[string]string{
		"spark.conf.key": "value",
	}

	//// Configure SparkApplication Spec
	sparkAppMeta := v1.ObjectMeta{
		Name:      "overriden",
		Namespace: "default",
		Labels:    labels,
	}

	driverPodSpec := v1beta2.SparkPodSpec{
		Cores:  &driverCores,
		Memory: &driverMemory,
	}

	executorPodSpec := v1beta2.SparkPodSpec{
		Cores:  &executorCores,
		Memory: &executorMemory,
	}

	driverSpec := v1beta2.DriverSpec{
		SparkPodSpec: driverPodSpec,
	}

	executorSpec := v1beta2.ExecutorSpec{
		SparkPodSpec: executorPodSpec,
	}

	dependencies := v1beta2.Dependencies{
		Jars: jars,
	}

	dynamicAllocation := v1beta2.DynamicAllocation{
		Enabled:          true,
		InitialExecutors: &initialExecutors,
		MinExecutors:     &minExecutors,
		MaxExecutors:     &maxExecutors,
	}

	sparkAppSpec := v1beta2.SparkApplicationSpec{
		Type:                appType,
		ProxyUser:           &proxyUser,
		Image:               nil,
		ImagePullSecrets:    nil,
		MainClass:           &className,
		MainApplicationFile: &file,
		Arguments:           args,
		SparkConf:           sparkConf,
		HadoopConf:          nil,
		SparkConfigMap:      nil,
		HadoopConfigMap:     nil,
		Volumes:             nil,
		Driver:              driverSpec,
		Executor:            executorSpec,
		Deps:                dependencies,
		NodeSelector:        nil,
		DynamicAllocation:   &dynamicAllocation,
	}

	sparkApp := v1beta2.SparkApplication{
		ObjectMeta: sparkAppMeta,
		Spec:       sparkAppSpec,
	}

	return &sparkApp

}
