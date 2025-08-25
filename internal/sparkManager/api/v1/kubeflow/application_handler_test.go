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

package v1kubeflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	sgMiddleware "github.com/slackhq/spark-gateway/internal/shared/middleware"
	"github.com/slackhq/spark-gateway/internal/sparkManager/service"
)

var expectedSparkApplication v1beta2.SparkApplication = v1beta2.SparkApplication{
	ObjectMeta: v1.ObjectMeta{
		Name:      "appName",
		Namespace: "testNamespace",
	},
	Status: v1beta2.SparkApplicationStatus{
		SubmissionID: "test123",
	}}

var logString string = "testlogstring"

var mockSparkAppService_SuccessTests service.SparkApplicationServiceMock = service.SparkApplicationServiceMock{
	GetFunc: func(namespace string, name string) (*v1beta2.SparkApplication, error) {
		return &expectedSparkApplication, nil
	},
	StatusFunc: func(namespace string, name string) (*v1beta2.SparkApplicationStatus, error) {
		return &expectedSparkApplication.Status, nil
	},
	LogsFunc: func(namespace string, name string, tailLines int64) (*string, error) {
		return &logString, nil
	},
	CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return &expectedSparkApplication, nil
	},
	DeleteFunc: func(ctx context.Context, namespace string, name string) error {
		return nil
	},
}

var mockSparkAppService_FailureTests service.SparkApplicationServiceMock = service.SparkApplicationServiceMock{
	GetFunc: func(namespace string, name string) (*v1beta2.SparkApplication, error) {
		return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s'", expectedSparkApplication.Name))
	},
	StatusFunc: func(namespace string, name string) (*v1beta2.SparkApplicationStatus, error) {
		return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s'", expectedSparkApplication.Name))
	},
	LogsFunc: func(namespace string, name string, tailLines int64) (*string, error) {
		return nil, gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s' to get Spark Driver Pod name for logs", expectedSparkApplication.Name))
	},
	CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
		return nil, gatewayerrors.NewAlreadyExists(errors.New("resource.group \"test\" already exists"))
	},
	DeleteFunc: func(ctx context.Context, namespace string, name string) error {
		return gatewayerrors.NewNotFound(fmt.Errorf("error getting SparkApplication '%s'", expectedSparkApplication.Name))
	},
}

var testConfig = &config.SparkGatewayConfig{DefaultLogLines: 100}

func init() {
	gin.SetMode(gin.TestMode)
}

func NewV1Router(mockService *service.SparkApplicationServiceMock) *gin.Engine {
	router := gin.Default()
	v1Group := router.Group("/api/v1")
	v1Group.Use(sgMiddleware.ApplicationErrorHandler)

	RegisterKubeflowApplicationRoutes(v1Group, testConfig, mockService)

	return router
}

func Test_SparkApplicationHandler_Get_Success(t *testing.T) {

	ginRouter := NewV1Router(&mockSparkAppService_SuccessTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName", nil)
	ginRouter.ServeHTTP(w, req)

	var respBody *v1beta2.SparkApplication
	json.Unmarshal(w.Body.Bytes(), &respBody)

	assert.Equal(t, &expectedSparkApplication, respBody, "returned JSON should match")
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")

}

func Test_SparkApplicationHandler_Get_Error(t *testing.T) {

	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName", nil)
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"error getting SparkApplication 'appName'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}

func TestSparkApplicationHandler_Status_Success(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_SuccessTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName/status", nil)
	ginRouter.ServeHTTP(w, req)

	var respBody *v1beta2.SparkApplicationStatus
	json.Unmarshal(w.Body.Bytes(), &respBody)

	assert.Equal(t, &expectedSparkApplication.Status, respBody, "returned JSON should match")
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")

}

func Test_SparkApplicationHandler_Status_Error(t *testing.T) {

	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName/status", nil)
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"error getting SparkApplication 'appName'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}

func Test_SparkApplicationHandler_Logs_Success(t *testing.T) {

	ginRouter := NewV1Router(&mockSparkAppService_SuccessTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName/logs", nil)
	ginRouter.ServeHTTP(w, req)

	var respBody string
	json.Unmarshal(w.Body.Bytes(), &respBody)

	assert.Equal(t, logString, respBody, "returned JSON should match")
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")

}

func Test_SparkApplicationHandler_Logs_Error(t *testing.T) {

	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespace/appName/logs", nil)
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"error getting SparkApplication 'appName' to get Spark Driver Pod name for logs"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}

func TestSparkApplicationHandler_Create_Success(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_SuccessTests)

	jsonReq, _ := json.Marshal(&expectedSparkApplication)
	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/namespace/appName", bytes.NewBuffer(jsonReq))
	ginRouter.ServeHTTP(w, req)

	var respBody *v1beta2.SparkApplication
	json.Unmarshal(w.Body.Bytes(), &respBody)

	assert.Equal(t, &expectedSparkApplication, respBody, "returned JSON should match")
	assert.Equal(t, http.StatusCreated, w.Code, "codes should match")

}

func TestSparkApplicationHandler_Create_BadRequest(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/namespace/appName", nil)
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"invalid request"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusBadRequest, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}

func TestSparkApplicationHandler_Create_AlreadyExists(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	jsonReq, _ := json.Marshal(&expectedSparkApplication)
	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/namespace/appName", bytes.NewBuffer(jsonReq))
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"resource.group \"test\" already exists"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusConflict, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}

func TestSparkApplicationHandler_Delete_Success(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_SuccessTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/namespace/appName", nil)
	ginRouter.ServeHTTP(w, req)

	expectedResponse := `{"status":"success"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, expectedResponse, string(responseData), "returned JSON should match")

}

func TestSparkApplicationHandler_Delete_NotFound(t *testing.T) {
	ginRouter := NewV1Router(&mockSparkAppService_FailureTests)

	w := httptest.NewRecorder() // http.ResponseWriter
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/namespace/appName", nil)
	ginRouter.ServeHTTP(w, req)

	expectedErrorResp := `{"error":"error getting SparkApplication 'appName'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, expectedErrorResp, string(responseData), "errors should match")

}
