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
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pkgHttp "github.com/slackhq/spark-gateway/pkg/http"
)

//go:generate moq -rm -out mocksparkapplicationservice.go . SparkApplicationService

type SparkApplicationService interface {
	Get(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error)
	List(ctx context.Context, namespace string) ([]*metav1.ObjectMeta, error)
	Status(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplicationStatus, error)
	Logs(ctx context.Context, namespace string, name string, tailLines int64) (*string, error)
	Create(ctx context.Context, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error)
	Delete(ctx context.Context, namespace string, name string) error
}

const ApplicationAPIVersion = "v1beta2"

type SparkApplicationHandler struct {
	sparkApplicationService SparkApplicationService
	defaultLogLines         int
}

func NewSparkApplicationHandler(sparkApplicationService SparkApplicationService, defaultLogLines int) *SparkApplicationHandler {

	sparkApplicationHandler := SparkApplicationHandler{sparkApplicationService: sparkApplicationService, defaultLogLines: defaultLogLines}
	return &sparkApplicationHandler
}

func (h SparkApplicationHandler) RegisterRoutes(rg *gin.RouterGroup) {
	nsGroup := rg.Group(ApplicationAPIVersion)
	nsGroup.Use(pkgHttp.ApplicationErrorHandler)
	{
		nsGroup.GET("/:namespace", h.List)

		nsGroup.POST("/:namespace/:name", h.Create)
		nsGroup.GET("/:namespace/:name", h.Get)
		nsGroup.GET("/:namespace/:name/status", h.Status)
		nsGroup.GET("/:namespace/:name/logs", h.Logs)

		nsGroup.DELETE("/:namespace/:name", h.Delete)
	}
}

func (h *SparkApplicationHandler) Get(c *gin.Context) {

	application, err := h.sparkApplicationService.Get(c, c.Param("namespace"), c.Param("name"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

func (h *SparkApplicationHandler) List(c *gin.Context) {
	application, err := h.sparkApplicationService.List(c, c.Param("namespace"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

func (h *SparkApplicationHandler) Status(c *gin.Context) {

	appStatus, err := h.sparkApplicationService.Status(c, c.Param("namespace"), c.Param("name"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appStatus)
}

func (h *SparkApplicationHandler) Logs(c *gin.Context) {

	tailLinesStr := c.DefaultQuery("lines", strconv.Itoa(h.defaultLogLines))
	tailLines, err := strconv.ParseInt(tailLinesStr, 10, 64)
	if err != nil {
		c.Error(fmt.Errorf("cannot parse tailLines to int: %v, %w", tailLinesStr, err))
		return
	}

	logs, err := h.sparkApplicationService.Logs(c, c.Param("namespace"), c.Param("name"), tailLines)
	if err != nil {
		c.Error(fmt.Errorf("cannot get logs: %w", err))
		return
	}

	c.JSON(http.StatusOK, logs)

}

func (h *SparkApplicationHandler) Create(c *gin.Context) {
	var application v1beta2.SparkApplication

	if err := c.ShouldBindJSON(&application); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sparkApplication, err := h.sparkApplicationService.Create(c, &application)

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, sparkApplication)
}

func (h *SparkApplicationHandler) Delete(c *gin.Context) {

	err := h.sparkApplicationService.Delete(c, c.Param("namespace"), c.Param("name"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
