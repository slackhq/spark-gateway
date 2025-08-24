package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/api/health"
	"github.com/slackhq/spark-gateway/internal/gateway/api/middleware"
	"github.com/slackhq/spark-gateway/internal/gateway/api/swagger"
	v1kubeflow "github.com/slackhq/spark-gateway/internal/gateway/api/v1/kubeflow"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
)

func NewRouter(sgConf *config.SparkGatewayConfig, appService service.GatewayApplicationService) (*gin.Engine, error) {

	router := gin.Default()
	router.Use(sgHttp.ApplicationErrorHandler)

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

	if sgConf.GatewayConfig.EnableSwaggerUI {
		swagger.RegisterSwaggerRoutes(rootGroup)
	}

	// Versioned routes
	v1Group := router.Group("/v1")
	if err := middleware.AddMiddleware(sgConf.GatewayConfig.Middleware, v1Group); err != nil {
		return nil, fmt.Errorf("error adding middlewares to routes: %w", err)
	}

	v1kubeflow.RegisterKubeflowApplicationRoutes(v1Group, sgConf, appService)

	return router, nil

}
