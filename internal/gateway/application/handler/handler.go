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
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	pkgHttp "github.com/slackhq/spark-gateway/pkg/http"
	"github.com/slackhq/spark-gateway/pkg/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//go:generate moq -rm  -out mockgatewayapplicationservice.go . GatewayApplicationService

type GatewayApplicationService interface {
	Get(ctx context.Context, gatewayId string) (*model.GatewayApplication, error)
	List(ctx context.Context, cluster string, namespace string) ([]*metav1.ObjectMeta, error)
	Create(ctx context.Context, application *v1beta2.SparkApplication, user string) (*model.GatewayApplication, error)
	Status(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error)
	Logs(ctx context.Context, gatewayId string, tailLines int) (*string, error)
	Delete(ctx context.Context, gatewayId string) error
}

type ApplicationHandler struct {
	service         GatewayApplicationService
	defaultLogLines int
}

func NewApplicationHandler(service GatewayApplicationService, defaultLogLines int) *ApplicationHandler {
	return &ApplicationHandler{service: service, defaultLogLines: defaultLogLines}
}

func (h ApplicationHandler) RegisterRoutes(rg *gin.RouterGroup) {

	appGroup := rg.Group("/applications")
	appGroup.Use(pkgHttp.ApplicationErrorHandler)
	{

		appGroup.GET("", h.List)
		appGroup.POST("", h.Create)

		appGroup.GET("/:gatewayId", h.Get)
		appGroup.DELETE("/:gatewayId", h.Delete)

		appGroup.GET("/:gatewayId/status", h.Status)
		appGroup.GET("/:gatewayId/logs", h.Logs)

	}
}

func (h ApplicationHandler) List(c *gin.Context) {

	cluster := c.Query("cluster")
	if cluster == "" {
		c.Error(gatewayerrors.NewBadRequest(errors.New("must provide 'cluster' query parameter and/or 'namespace' query parameter")))
		return
	}

	namespace := c.Query("namespace")

	applications, err := h.service.List(c, cluster, namespace)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, applications)
}

func (h ApplicationHandler) Get(c *gin.Context) {

	application, err := h.service.Get(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

func (h ApplicationHandler) Status(c *gin.Context) {

	appStatus, err := h.service.Status(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appStatus)
}

func (h ApplicationHandler) Logs(c *gin.Context) {

	tailLines := h.defaultLogLines
	var err error
	tailLinesQuery := c.Query("lines")
	if tailLinesQuery != "" {
		tailLines, err = strconv.Atoi(tailLinesQuery)
		if err != nil {
			c.Error(err)
			return
		}
	}
	logString, err := h.service.Logs(c, c.Param("gatewayId"), tailLines)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, logString)
}

func (h ApplicationHandler) Create(c *gin.Context) {

	var app v1beta2.SparkApplication

	if err := c.ShouldBindJSON(&app); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Set user
	// This should always be set due to prior auth middleware
	currentUser, _ := c.Get("user")
	user := currentUser.(string)

	application, err := h.service.Create(c, &app, user)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, application)
}

func (h ApplicationHandler) Delete(c *gin.Context) {

	err := h.service.Delete(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
