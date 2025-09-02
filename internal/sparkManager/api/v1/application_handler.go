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
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"

	"github.com/slackhq/spark-gateway/internal/sparkManager/service"
)

type SparkApplicationHandler struct {
	sparkApplicationService service.SparkApplicationService
	defaultLogLines         int
}

func NewSparkApplicationHandler(sparkApplicationService service.SparkApplicationService, defaultLogLines int) *SparkApplicationHandler {

	sparkApplicationHandler := SparkApplicationHandler{sparkApplicationService: sparkApplicationService, defaultLogLines: defaultLogLines}
	return &sparkApplicationHandler
}

func (h *SparkApplicationHandler) Get(c *gin.Context) {

	application, err := h.sparkApplicationService.Get(c.Param("namespace"), c.Param("name"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, application)
}

func (h *SparkApplicationHandler) List(c *gin.Context) {
	appMetaList, err := h.sparkApplicationService.List(c.Param("namespace"))

	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appMetaList)
}

func (h *SparkApplicationHandler) Status(c *gin.Context) {

	appStatus, err := h.sparkApplicationService.Status(c.Param("namespace"), c.Param("name"))

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

	logs, err := h.sparkApplicationService.Logs(c.Param("namespace"), c.Param("name"), tailLines)
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
