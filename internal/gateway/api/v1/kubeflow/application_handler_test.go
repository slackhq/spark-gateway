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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var testConfig = &config.SparkGatewayConfig{DefaultLogLines: 100}

func init() {
	gin.SetMode(gin.TestMode)

}

func NewV1Router() (*gin.Engine, *gin.RouterGroup) {
	router := gin.Default()
	v1Group := router.Group("/api/v1")
	v1Group.Use(sgHttp.ApplicationErrorHandler)

	return router, v1Group
}

var errorHandlerTests = []struct {
	test       string
	err        error
	returnJSON string
	statusCode int
}{
	{
		test:       "app not found err",
		err:        gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'")),
		returnJSON: `{"error":"error getting SparkApplication 'clusterid-testid'"}`,
		statusCode: http.StatusNotFound,
	},
	{
		test:       "already exists",
		err:        gatewayerrors.NewAlreadyExists(errors.New("resource.group \"test\" already exists")),
		returnJSON: `{"error":"resource.group \"test\" already exists"}`,
		statusCode: http.StatusConflict,
	},
	{
		test:       "internal server error",
		err:        errors.New("test error"),
		returnJSON: `{"error":"test error"}`,
		statusCode: http.StatusInternalServerError,
	},
}

func TestApplicationHandlerErrorHandler(t *testing.T) {

	for _, test := range errorHandlerTests {
		t.Run(test.test, func(t *testing.T) {
			router := gin.New()
			router.Use(sgHttp.ApplicationErrorHandler)
			router.Use(func(ctx *gin.Context) {
				ctx.Error(test.err)
			})
			router.GET("/", func(ctx *gin.Context) {})
			req, _ := http.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			responseData, _ := io.ReadAll(w.Body)
			assert.Equal(t, test.statusCode, w.Code, "codes should match")
			assert.Equal(t, test.returnJSON, string(responseData), "returned JSON should match")

		})
	}
}

func TestApplicationHandlerGet(t *testing.T) {

	retApp := &domain.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-testid",
				Namespace: "test",
			},
		},
		Cluster: "cluster",
		User:    "user",
	}

	service := &service.GatewayApplicationServiceMock{
		GetFunc: func(ctx context.Context, gatewayId string) (*domain.GatewayApplication, error) {
			return retApp, nil
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}

func TestApplicationHandlerGetError(t *testing.T) {

	service := &service.GatewayApplicationServiceMock{
		GetFunc: func(ctx context.Context, gatewayId string) (*domain.GatewayApplication, error) {
			return &domain.GatewayApplication{}, gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	var gotApp domain.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}
func TestApplicationHandlerStatus(t *testing.T) {

	retResp := &v1beta2.SparkApplicationStatus{
		SubmissionID: "submissionId",
	}

	service := &service.GatewayApplicationServiceMock{
		StatusFunc: func(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error) {
			return retResp, nil
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotStatus v1beta2.SparkApplicationStatus
	json.Unmarshal(w.Body.Bytes(), &gotStatus)

	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, gotStatus, *retResp, "returned JSON should match")
}
func TestApplicationHandlerStatusError(t *testing.T) {

	service := &service.GatewayApplicationServiceMock{
		StatusFunc: func(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error) {
			return &v1beta2.SparkApplicationStatus{}, gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}

func TestApplicationHandlerCreate(t *testing.T) {
	router, v1Group := NewV1Router()

	v1Group.Use(func(ctx *gin.Context) {
		ctx.Set("user", "user")
		ctx.Next()
	})

	retApp := &domain.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-testid",
				Namespace: "test",
			},
		},
		Cluster: "cluster",
		User:    "user",
	}

	service := &service.GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*domain.GatewayApplication, error) {
			return retApp, nil
		},
	}

	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	createApp := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "clusterid-testid",
			Namespace: "test",
		},
	}

	jsonReq, _ := json.Marshal(createApp)
	req, _ := http.NewRequest("POST", "/api/v1/applications", bytes.NewBuffer(jsonReq))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusCreated, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}
func TestApplicationHandlerCreateBadRequest(t *testing.T) {

	service := &service.GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*domain.GatewayApplication, error) {
			return nil, nil
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("POST", "/api/v1/applications", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"invalid request"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusBadRequest, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}

func TestApplicationHandlerCreateAlreadyExists(t *testing.T) {
	router, v1Group := NewV1Router()

	v1Group.Use(func(ctx *gin.Context) {
		ctx.Set("user", "user")
		ctx.Next()
	})

	service := &service.GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*domain.GatewayApplication, error) {
			return nil, gatewayerrors.NewAlreadyExists(errors.New("resource.group \"test\" already exists"))
		},
	}

	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	createReq := &v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "clusterid-testid",
			Namespace: "test",
		},
	}

	jsonReq, _ := json.Marshal(createReq)
	req, _ := http.NewRequest("POST", "/api/v1/applications", bytes.NewBuffer(jsonReq))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"resource.group \"test\" already exists"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusConflict, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}

func TestApplicationHandlerDelete(t *testing.T) {

	service := &service.GatewayApplicationServiceMock{
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			return nil
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("DELETE", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	resp := `{"status":"success"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "returned JSON should match")
}
func TestApplicationHandlerDeleteError(t *testing.T) {

	service := &service.GatewayApplicationServiceMock{
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			return gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}

	router, v1Group := NewV1Router()
	RegisterKubeflowApplicationRoutes(v1Group, testConfig, service)

	req, _ := http.NewRequest("DELETE", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "returned JSON should match")
}
