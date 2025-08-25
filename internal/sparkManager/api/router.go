package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/sparkManager/api/health"
	v1kubeflow "github.com/slackhq/spark-gateway/internal/sparkManager/api/v1/kubeflow"
	"github.com/slackhq/spark-gateway/internal/sparkManager/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
)

func NewRouter(sgConf *config.SparkGatewayConfig, appService service.ApplicationService) (*gin.Engine, error) {

	router := gin.Default()
	router.Use(sgHttp.ApplicationErrorHandler)

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

	// Versioned routes
	v1Group := router.Group("/v1")
	if err := middleware.AddMiddleware(sgConf.GatewayConfig.Middleware, v1Group); err != nil {
		return nil, fmt.Errorf("error adding middlewares to routes: %w", err)
	}

	v1kubeflow.RegisterKubeflowApplicationRoutes(v1Group, sgConf, appService)

	return router, nil

}
