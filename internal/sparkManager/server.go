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

package sparkManager

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	sparkClientSet "github.com/kubeflow/spark-operator/v2/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/sparkManager/api/v1/application/handler"
	appRepo "github.com/slackhq/spark-gateway/internal/sparkManager/api/v1/application/repository"
	"github.com/slackhq/spark-gateway/internal/sparkManager/api/v1/application/service"
	"github.com/slackhq/spark-gateway/internal/sparkManager/metrics"
	dbRepo "github.com/slackhq/spark-gateway/pkg/database/repository"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/kube"

	"time"

	"github.com/gin-gonic/gin"

	"github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/http/health"
)

type SparkManager struct {
	httpServer    *http.Server
	metricsServer *metrics.Handler
	ctx           context.Context
}

func NewSparkManager(ctx context.Context, sgConfig *config.SparkGatewayConfig, cluster string) (*SparkManager, error) {
	// Get KubeCluster configs
	kubeCluster := sgConfig.GetKubeCluster(cluster)
	if kubeCluster == nil {
		return nil, fmt.Errorf("%s cluster information not found in config file", cluster)
	}

	// Creates a gin router with default middleware:
	// logger and recovery (crash-free) middleware
	ginRouter := gin.Default()

	// Setup unversioned services/handlers first
	rootGroup := ginRouter.Group("")
	
	healthHandler := health.NewHealthHandler(health.NewHealthService())
	healthHandler.RegisterRoutes(rootGroup)

	// Create /api group where all versioned endpoints will attach themselves
	apiGroup := ginRouter.Group("/api")


	// Create DB Repo
	var database dbRepo.DatabaseRepository = nil
	if sgConfig.SparkManagerConfig.Database.Enable {
		database = dbRepo.NewDatabase(ctx, sgConfig.SparkManagerConfig.Database)
	}

	// Initialize Kube Clients
	kubeConfig, err := kube.GetKubeConfig(sgConfig.SparkManagerConfig.ClusterAuthType, kubeCluster)
	if err != nil {
		return nil, err
	}
	k8sClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error creating k8s client: %w", err))
	}
	sparkClient, err := sparkClientSet.NewForConfig(kubeConfig)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error creating spark client: %w", err))
	}

	// Initialize Kube SparkApp Controller
	controller, err := kube.NewSparkController(
		ctx,
		kubeConfig,
		sgConfig.SelectorKey,
		sgConfig.SelectorValue,
		kubeCluster.Name,
		database,
	)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("unable to initialize SparkApplication Controller: %w", err))
	}

	// Init repos
	sparkAppRepo, err := appRepo.NewSparkApplicationRepository(controller, sparkClient, k8sClient)
	if err != nil {
		return nil, fmt.Errorf("unable to create NewSparkApplicationRepository: %w", err)
	}
	metricsRepo := metrics.NewRepository(controller)

	// Initialize services
	sparkApplicationService := service.NewSparkApplicationService(sparkAppRepo, database, *kubeCluster)
	metricsService := metrics.NewService(metricsRepo, kubeCluster)

	// Init handlers
	sparkAppHandler := handler.NewSparkApplicationHandler(sparkApplicationService, sgConfig.DefaultLogLines)
	metricsServer := metrics.NewHandler(metricsService, sgConfig.SparkManagerConfig.MetricsServer)

	// Register routes
	sparkAppHandler.RegisterRoutes(apiGroup)
	healthHandler.RegisterRoutes(apiGroup)

	server := http.Server{
		Addr:    fmt.Sprintf(":%s", sgConfig.SparkManagerPort),
		Handler: ginRouter,
	}

	return &SparkManager{
		httpServer:    &server,
		metricsServer: metricsServer,
		ctx:           ctx,
	}, nil

}

func (server *SparkManager) Run() {
	klog.Infof("http server listening %s", server.httpServer.Addr)
	klog.Infof("metrics server listening %s", server.metricsServer.Server.Addr)

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := server.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Error(err)
		}
	}()

	server.metricsServer.Run(server.ctx)

	<-server.ctx.Done()

	klog.Info("Shutting down SparkManager...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.httpServer.Shutdown(timeoutCtx); err != nil {
		klog.Fatal("Server forced to shutdown:", err)
	}

	klog.Infoln("SparkManager exiting, bye")
}
