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
	"fmt"
	swaggerDocs "github.com/slackhq/spark-gateway/docs/swagger"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	pkgHttp "github.com/slackhq/spark-gateway/pkg/http"
	"github.com/slackhq/spark-gateway/pkg/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//  Swagger General	API Info
//	@title			Spark Gateway
//	@version		1.0
//	@description	REST API for managing SparkApplication resources across multiple clusters
//	@securityDefinitions.basic	BasicAuth

//  @license.name	Apache 2.0
//  @license.url	http://www.apache.org/licenses/LICENSE-2.0.html

const ApplicationAPIVersion = "v1"
const SparkApplicationPathName = "applications"

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

func (h *ApplicationHandler) RegisterRoutes(rg *gin.RouterGroup) {

	appGroup := rg.Group(fmt.Sprintf("/%s/%s", ApplicationAPIVersion, SparkApplicationPathName))
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

// ListSparkApplications godoc
// @Summary List SparkApplications
// @Description Lists SparkApplications metadata in the specified cluster. Optionally filter by namespace.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param cluster query string true "Cluster name"
// @Param namespace query string false "Namespace (optional)"
// @Success 200 {array} metav1.ObjectMeta "List of SparkApplication metadata"
// @Router / [get]
func (h *ApplicationHandler) List(c *gin.Context) {

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

// GetSparkApplication godoc
// @Summary Get a SparkApplication
// @Description Retrieves the full SparkApplication resource by ID.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "SparkApplication Name"
// @Success 200 {object} model.GatewayApplication "SparkApplication resource"
// @Router /{gatewayId} [get]
func (h *ApplicationHandler) Get(c *gin.Context) {

	application, err := h.service.Get(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

// GetSparkApplicationStatus godoc
// @Summary Get SparkApplication status
// @Description Retrieves only the status field of a SparkApplication.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "SparkApplication Name"
// @Success 200 {object} v1beta2.SparkApplicationStatus "SparkApplication status"
// @Router /{gatewayId}/status [get]
func (h *ApplicationHandler) Status(c *gin.Context) {

	appStatus, err := h.service.Status(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appStatus)
}

// GetSparkApplicationLogs godoc
// @Summary Get driver logs of a SparkApplication
// @Description Retrieves the last N lines of driver logs for the specified SparkApplication. Defaults to the last 100 lines.
// @Tags Applications
// @Accept json
// @Produce plain
// @Security BasicAuth
// @Param gatewayId path string true "SparkApplication Name"
// @Param lines query int false "Number of log lines to retrieve (default: 100)"
// @Success 200 {string} string "Driver logs"
// @Router /{gatewayId}/logs [get]
func (h *ApplicationHandler) Logs(c *gin.Context) {

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

// CreateSparkApplication godoc
// @Summary Submit a new SparkApplication
// @Description Submits the provided SparkApplication to the given namespace.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param SparkApplication body v1beta2.SparkApplication true "v1beta2.SparkApplication resource"
// @Success 201 {object} model.GatewayApplication "SparkApplication Created"
// @Router / [post]
func (h *ApplicationHandler) Create(c *gin.Context) {

	var app v1beta2.SparkApplication

	if err := c.ShouldBindJSON(&app); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Set user
	// This should always exist because of prior auth middlewares
	gotUser, exists := c.Get("user")
	if !exists {
		c.Error(errors.New("no user set, congratulations you've encountered a bug that should never happen"))
	}
	user := gotUser.(string)

	application, err := h.service.Create(c, &app, user)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, application)
}

// DeleteSparkApplication godoc
// @Summary Delete a SparkApplication
// @Description Deletes the specified SparkApplication
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "SparkApplication Name"
// @Success 200 {object} map[string]string "Application deleted: {'status': 'success'}"
// @Router /{gatewayId} [delete]
func (h *ApplicationHandler) Delete(c *gin.Context) {

	err := h.service.Delete(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RegisterSwaggerDocs(rg *gin.RouterGroup) {
	swaggerDocs.SwaggerInfo.BasePath = fmt.Sprintf("/%s/%s", ApplicationAPIVersion, SparkApplicationPathName)

	// Swagger UI on /swagger/index.html
	rg.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, ginSwagger.DefaultModelsExpandDepth(-1)))
	// Redirect /doc and /docs/ to /swagger/index.html
	rg.GET("/docs", func(ctx *gin.Context) {
		ctx.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})

}
