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

package handler

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
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	pkgHttp "github.com/slackhq/spark-gateway/pkg/http"
	"github.com/slackhq/spark-gateway/pkg/model"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	gin.SetMode(gin.TestMode)
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
			router.Use(pkgHttp.ApplicationErrorHandler)
			router.Use(func(ctx *gin.Context) {
				ctx.Error(test.err)
				return
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
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	retApp := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-testid",
				Namespace: "test",
			},
		},
		Cluster: "cluster",
		User:    "user",
	}

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		GetFunc: func(ctx context.Context, gatewayId string) (*model.GatewayApplication, error) {
			return retApp, nil
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp model.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}
func TestApplicationHandlerGetError(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		GetFunc: func(ctx context.Context, gatewayId string) (*model.GatewayApplication, error) {
			return &model.GatewayApplication{}, gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	var gotApp model.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}
func TestApplicationHandlerStatus(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	retResp := &v1beta2.SparkApplicationStatus{
		SubmissionID: "submissionId",
	}

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		StatusFunc: func(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error) {
			return retResp, nil
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotStatus v1beta2.SparkApplicationStatus
	json.Unmarshal(w.Body.Bytes(), &gotStatus)

	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, gotStatus, *retResp, "returned JSON should match")
}
func TestApplicationHandlerStatusError(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		StatusFunc: func(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error) {
			return &v1beta2.SparkApplicationStatus{}, gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("GET", "/api/v1/applications/clusterid-testid/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}

func TestApplicationHandlerCreate(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)
	root.Use(func(ctx *gin.Context) {
		ctx.Set("user", "user")
		ctx.Next()
	})

	retApp := &model.GatewayApplication{
		SparkApplication: &v1beta2.SparkApplication{
			ObjectMeta: v1.ObjectMeta{
				Name:      "clusterid-testid",
				Namespace: "test",
			},
		},
		Cluster: "cluster",
		User:    "user",
	}

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*model.GatewayApplication, error) {
			return retApp, nil
		},
	}, 100)

	handler.RegisterRoutes(root)

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

	var gotApp model.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusCreated, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}
func TestApplicationHandlerCreateBadRequest(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*model.GatewayApplication, error) {
			return nil, nil
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("POST", "/api/v1/applications", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"error":"invalid request"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusBadRequest, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "errors should match")
}

func TestApplicationHandlerCreateAlreadyExists(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)
	root.Use(func(ctx *gin.Context) {
		ctx.Set("user", "user")
		ctx.Next()
	})

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, user string) (*model.GatewayApplication, error) {
			return nil, gatewayerrors.NewAlreadyExists(errors.New("resource.group \"test\" already exists"))
		},
	}, 100)

	handler.RegisterRoutes(root)

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
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			return nil
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("DELETE", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp model.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	resp := `{"status":"success"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "returned JSON should match")
}
func TestApplicationHandlerDeleteError(t *testing.T) {
	router := gin.New()
	root := router.Group("api")
	router.Use(pkgHttp.ApplicationErrorHandler)

	handler := NewApplicationHandler(&GatewayApplicationServiceMock{
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			return gatewayerrors.NewNotFound(errors.New("error getting SparkApplication 'clusterid-testid'"))
		},
	}, 100)

	handler.RegisterRoutes(root)

	req, _ := http.NewRequest("DELETE", "/api/v1/applications/clusterid-testid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp model.GatewayApplication
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	resp := `{"error":"error getting SparkApplication 'clusterid-testid'"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusNotFound, w.Code, "codes should match")
	assert.Equal(t, resp, string(responseData), "returned JSON should match")
}
