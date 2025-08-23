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

package health

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthService interface {
	Health(ctx context.Context) HealthResponse
}

type HealthHandler struct {
	service HealthService
}

func NewHealthHandler(service HealthService) *HealthHandler {
	healthHandler := HealthHandler{service: service}
	return &healthHandler
}

func (h *HealthHandler) RegisterRoutes(rg *gin.RouterGroup) {
	healthGroup := rg.Group("")

	healthGroup.GET("/health", h.Health)
}

func (h *HealthHandler) Health(c *gin.Context) {

	health := h.service.Health(c)

	c.JSON(http.StatusOK, health)
}
