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

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/gateway/application/handler"
	"github.com/slackhq/spark-gateway/internal/gateway/application/repository"
	"github.com/slackhq/spark-gateway/internal/gateway/application/service"
	"github.com/slackhq/spark-gateway/internal/gateway/cluster"

	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/slackhq/spark-gateway/internal/gateway/middleware"
	"github.com/slackhq/spark-gateway/internal/gateway/router"
	cfg "github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/model"

	"github.com/slackhq/spark-gateway/pkg/http/health"
)

type GatewayServer struct {
	httpServer *http.Server
	ctx        context.Context
}

func GenUUIDv7() (string, error) {
	uuid, err := uuid.NewV7()

	if err != nil {
		return "", err
	}

	return uuid.String(), nil
}

func NewGateway(ctx context.Context, sgConfig *cfg.SparkGatewayConfig, sparkManagerHostnameTemplate string) (*GatewayServer, error) {

	ginRouter := gin.Default()

	//Repos
	sparkManagerRepo, err := repository.NewSparkManagerRepository(sparkManagerHostnameTemplate, sgConfig.SparkManagerPort, sgConfig.DebugPorts)
	if err != nil {
		return nil, fmt.Errorf("could not create SparkManagerRespository: %w", err)
	}
	klog.Infof("Spark Gateway configured with SparkManagerRespository: %s", reflect.TypeOf(sparkManagerRepo).String())

	localClusterRepo, err := cluster.NewLocalClusterRepo(sgConfig.KubeClusters)
	if err != nil {
		return nil, fmt.Errorf("could not create LocalClusterRepo: %w", err)
	}
	klog.Infof("Spark Gateway configured with ClusterRepository: %s", reflect.TypeOf(sparkManagerRepo).String())

	clusterRouter, err := router.GetClusterRouter(
		sgConfig.ClusterRouter.Type,
		localClusterRepo,
		sgConfig.ClusterRouter,
		sparkManagerHostnameTemplate,
		sgConfig.SparkManagerConfig.MetricsServer,
		sgConfig.DebugPorts)
	if err != nil {
		return nil, err
	}

	fallbackClusterRouter, err := router.GetClusterRouter(
		sgConfig.ClusterRouter.FallbackType,
		localClusterRepo,
		sgConfig.ClusterRouter,
		sparkManagerHostnameTemplate,
		sgConfig.SparkManagerConfig.MetricsServer,
		sgConfig.DebugPorts)
	if err != nil {
		return nil, err
	}

	// Services
	appService := service.NewApplicationService(
		sparkManagerRepo,
		localClusterRepo,
		clusterRouter,
		fallbackClusterRouter,
		sgConfig.GatewayConfig,
		sgConfig.SelectorKey,
		sgConfig.SelectorValue,
		model.GatewayIdGenerator{UuidGenerator: GenUUIDv7},
	)

	healthService := health.NewHealthService()

	// Handlers
	appHandler := handler.NewApplicationHandler(appService, sgConfig.DefaultLogLines)

	healthHandler := health.NewHealthHandler(healthService)

	/// Authed
	/// Auth middlewares
	mwHandlerChain, err := middleware.AddMiddleware(sgConfig.GatewayConfig.Middleware)

	/// Register unversioned handlers
	rootGroup := ginRouter.Group("")
	healthHandler.RegisterRoutes(rootGroup)

	// Swagger UI
	if sgConfig.GatewayConfig.EnableSwaggerUI {
		handler.RegisterSwaggerDocs(rootGroup, sgConfig.GatewayConfig.GatewayApiVersion)
	}

	/// Register versioned handlers
	versionGroup := ginRouter.Group(fmt.Sprintf("/%s", sgConfig.GatewayConfig.GatewayApiVersion), mwHandlerChain...)
	appHandler.RegisterRoutes(versionGroup)

	// Log the routes after all routes are registered
	routes := ginRouter.Routes()
	klog.Infof("Registered Routes:")
	for _, route := range routes {
		klog.Infof("%s %s\n", route.Method, route.Path)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%s", sgConfig.GatewayConfig.GatewayPort),
		Handler: ginRouter,
	}

	return &GatewayServer{
		httpServer: &server,
		ctx:        ctx,
	}, nil
}

func (s GatewayServer) Run() {

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Infof("listen: %s\n", err)
		}
	}()

	<-s.ctx.Done()

	klog.Infof("Shutting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(timeoutCtx); err != nil {
		klog.Fatal("server forced to shutdown:", err)
	}

	klog.Infoln("Server exiting, bye")

}
