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

package livy

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

// validateIntParam extracts and validates an integer parameter from URL path or query string
func validateIntParam(c *gin.Context, paramName string, isPathParam bool, required bool) (int, bool) {
	var paramValue string
	if isPathParam {
		paramValue = c.Param(paramName)
	} else {
		paramValue = c.Query(paramName)
		if paramValue == "" && !required {
			return 0, true // Optional query parameter, return success with 0 value
		}
	}

	value, err := strconv.Atoi(paramValue)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": paramName + " must be an int"})
		return 0, false
	}
	if value < 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": paramName + " must be greater than or equal to 0"})
		return 0, false
	}
	return value, true
}

// resolveProxyUser determines the proxy user for a Livy batch request
// Priority order: doAs query parameter > request proxyUser field > authenticated user
func resolveProxyUser(c *gin.Context, req *domain.LivyCreateBatchRequest) error {
	// Check for doAs query parameter - this takes highest priority
	if doAs := c.Query("doAs"); doAs != "" {
		req.ProxyUser = doAs
		return nil
	}

	// If proxyUser is already set in the request, use it
	if req.ProxyUser != "" {
		return nil
	}

	// Fall back to the authenticated user from the context
	gotUser, exists := c.Get("user")
	if !exists {
		return errors.New("no user set, congratulations you've encountered a bug that should never happen")
	}

	req.ProxyUser = gotUser.(string)
	return nil
}

type LivyHandler struct {
	livyService service.LivyApplicationService
}

func NewLivyBatchApplicationHandler(livyService service.LivyApplicationService) *LivyHandler {
	return &LivyHandler{
		livyService: livyService,
	}
}

// GetLivyBatch godoc
// @Summary Get a Livy batch
// @Description Retrieves a Livy batch by ID.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param batchId path int true "Batch ID"
// @Success 200 {object} domain.LivyBatch "Livy batch details"
// @Router /batches/{batchId} [get]
func (l *LivyHandler) Get(c *gin.Context) {
	getId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	gotBatch, err := l.livyService.Get(c, getId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gotBatch)

}

// ListLivyBatches godoc
// @Summary List Livy batches
// @Description Lists Livy batches with optional pagination.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param from query int false "Starting index for pagination (default: 0)"
// @Param size query int false "Number of batches to return (default: 0 for all)"
// @Success 200 {object} domain.LivyListBatchesResponse "List of Livy batches"
// @Router /batches [get]
func (l *LivyHandler) List(c *gin.Context) {
	from, ok := validateIntParam(c, "from", false, false)
	if !ok {
		return
	}

	size, ok := validateIntParam(c, "size", false, false)
	if !ok {
		return
	}

	listBatches, err := l.livyService.List(c, from, size)
	if err != nil {
		c.Error(fmt.Errorf("error listing Livy SparkApplications: %w", err))
		return
	}

	c.JSON(http.StatusOK, domain.LivyListBatchesResponse{
		From:     from,
		Total:    len(listBatches),
		Sessions: listBatches,
	})

}

// CreateLivyBatch godoc
// @Summary Create a new Livy batch
// @Description Submits a new Livy batch request. Proxy user can be specified via doAs query parameter, request body, or authenticated user.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param doAs query string false "Proxy user to submit the batch as"
// @Param X-Spark-Gateway-Livy-Namespace header string false "Kubernetes namespace for the batch"
// @Param LivyCreateBatchRequest body domain.LivyCreateBatchRequest true "Livy batch request"
// @Success 201 {object} domain.LivyBatch "Created Livy batch"
// @Router /batches [post]
func (l *LivyHandler) Create(c *gin.Context) {
	var createReq domain.LivyCreateBatchRequest
	if err := c.ShouldBindJSON(&createReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": err.Error()})
		return
	}

	// Resolve the proxy user for this request
	if err := resolveProxyUser(c, &createReq); err != nil {
		c.Error(err)
		return
	}

	// Get namespace from headers if supplied
	namespace := c.GetHeader("X-Spark-Gateway-Livy-Namespace")

	createdBatch, err := l.livyService.Create(c, createReq, namespace)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, createdBatch)
}

// DeleteLivyBatch godoc
// @Summary Delete a Livy batch
// @Description Deletes the specified Livy batch.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param batchId path int true "Batch ID"
// @Success 200 {object} map[string]string "Batch deleted: {'msg': 'deleted'}"
// @Router /batches/{batchId} [delete]
func (l *LivyHandler) Delete(c *gin.Context) {
	deleteId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	err := l.livyService.Delete(c, deleteId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}

// GetLivyBatchLogs godoc
// @Summary Get logs for a Livy batch
// @Description Retrieves the driver logs for the specified Livy batch.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param batchId path int true "Batch ID"
// @Param size query int false "Number of log lines to retrieve (default: 0 for all)"
// @Success 200 {object} domain.LivyLogBatchResponse "Livy batch logs"
// @Router /batches/{batchId}/log [get]
func (l *LivyHandler) Logs(c *gin.Context) {
	size, ok := validateIntParam(c, "size", false, false)
	if !ok {
		return
	}

	logsId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	logs, err := l.livyService.Logs(c, logsId, size)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, domain.LivyLogBatchResponse{
		Id:   logsId,
		From: -1,
		Size: size,
		Log:  logs,
	})
}

// GetLivyBatchState godoc
// @Summary Get state of a Livy batch
// @Description Retrieves only the state field of the specified Livy batch.
// @Tags Livy
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param batchId path int true "Batch ID"
// @Success 200 {object} domain.LivyGetBatchStateResponse "Livy batch state"
// @Router /batches/{batchId}/state [get]
func (l *LivyHandler) State(c *gin.Context) {
	getId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	gotBatch, err := l.livyService.Get(c, getId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, domain.LivyGetBatchStateResponse{
		Id:    getId,
		State: gotBatch.State,
	})

}

// LivyErrorHandler attempts to coerce the last error within gin.Context.Errors.Last to a
// GatewayError for proper HttpStatus attribution. If the error casting fails, it aborts the connection
// with the Error message and a 500 error
func LivyErrorHandler(c *gin.Context) {
	// Before request
	c.Next()
	// After request
	if len(c.Errors) != 0 {
		lastErr := c.Errors.Last()

		var gatewayError gatewayerrors.GatewayError
		if errors.As(lastErr, &gatewayError) {
			c.AbortWithStatusJSON(gatewayError.Status, gin.H{"msg": gatewayError.Error()})
			return
		}

		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"msg": lastErr.Error()})
		return

	}
}
