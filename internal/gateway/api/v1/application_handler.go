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

package v1

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

type GatewayApplicationHandler struct {
	service         service.GatewayApplicationService
	defaultLogLines int
}

func NewGatewayApplicationHandler(service service.GatewayApplicationService, defaultLogLines int) *GatewayApplicationHandler {
	return &GatewayApplicationHandler{service: service, defaultLogLines: defaultLogLines}
}

// ListGatewayApplicationSummaries godoc
// @Summary List GatewayApplicationSummary
// @Description Lists summaries of applications in specified cluster. Optionally filter by namespace.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param cluster query string true "Cluster name"
// @Param namespace query string false "Namespace (optional)"
// @Success 200 {array} domain.GatewayApplicationSummary "List of GatewayApplicationSummary objects"
// @Router /v1/applications [get]
func (h *GatewayApplicationHandler) List(c *gin.Context) {

	cluster := c.Query("cluster")
	if cluster == "" {
		c.Error(gatewayerrors.NewBadRequest(errors.New("must provide 'cluster' query parameter and/or 'namespace' query parameter")))
		return
	}

	namespace := c.Query("namespace")

	appMetaList, err := h.service.List(c, cluster, namespace)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appMetaList)
}

// GetGatewayApplication godoc
// @Summary Get a GatewayApplication
// @Description Retrieves the full GatewayApplication resource by ID.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "GatewayApplication Name"
// @Success 200 {object} domain.GatewayApplication "GatewayApplication resource"
// @Router /v1/applications/{gatewayId} [get]
func (h *GatewayApplicationHandler) Get(c *gin.Context) {

	application, err := h.service.Get(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

// GetGatewayApplicationStatus godoc
// @Summary Get GatewayApplication status
// @Description Retrieves only the status field of a GatewayApplication.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "GatewayApplication Name"
// @Success 200 {object} v1beta2.SparkApplicationStatus "GatewayApplication status"
// @Router /v1/applications/{gatewayId}/status [get]
func (h *GatewayApplicationHandler) Status(c *gin.Context) {

	appStatus, err := h.service.Status(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appStatus)
}

// GetGatewayApplicationLogs godoc
// @Summary Get driver logs of a GatewayApplication
// @Description Retrieves the last N lines of driver logs for the specified GatewayApplication. Defaults to the last 100 lines.
// @Tags Applications
// @Accept json
// @Produce plain
// @Security BasicAuth
// @Param gatewayId path string true "GatewayApplication Name"
// @Param lines query int false "Number of log lines to retrieve (default: 100)"
// @Success 200 {string} string "Driver logs"
// @Router /v1/applications/{gatewayId}/logs [get]
func (h *GatewayApplicationHandler) Logs(c *gin.Context) {

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

// CreateGatewayApplication godoc
// @Summary Submit a new GatewayApplication
// @Description Submits the provided GatewayApplication to the given namespace.
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param SparkApplication body v1beta2.SparkApplication true "v1beta2.SparkApplication resource"
// @Success 201 {object} domain.GatewayApplication "GatewayApplication Created"
// @Router /v1/applications/ [post]
func (h *GatewayApplicationHandler) Create(c *gin.Context) {

	var app v1beta2.SparkApplication

	if err := c.ShouldBindJSON(&app); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if app.Namespace == "" {
		c.Error(gatewayerrors.NewBadRequest(errors.New("submitted SparkApplication must have a Namespace")))
		return
	}

	gotUser, exists := c.Get("user")
	if !exists {
		c.Error(errors.New("no user set, congratulations you've encountered a bug that should never happen"))
		return
	}
	user := gotUser.(string)

	createdApp, err := h.service.Create(c, &app, user)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, createdApp)
}

// DeleteGatewayApplication godoc
// @Summary Delete a GatewayApplication
// @Description Deletes the specified GatewayApplication
// @Tags Applications
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param gatewayId path string true "GatewayApplication Name"
// @Success 200 {object} map[string]string "Application deleted: {'status': 'success'}"
// @Router /v1/applications/{gatewayId} [delete]
func (h *GatewayApplicationHandler) Delete(c *gin.Context) {

	err := h.service.Delete(c, c.Param("gatewayId"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
