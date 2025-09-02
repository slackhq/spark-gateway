package api

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	sgMiddleware "github.com/slackhq/spark-gateway/internal/shared/middleware"
	"github.com/slackhq/spark-gateway/internal/sparkManager/api/health"
	"github.com/slackhq/spark-gateway/internal/sparkManager/api/v1"
	"github.com/slackhq/spark-gateway/internal/sparkManager/service"
)

func NewRouter(sgConf *config.SparkGatewayConfig, appService service.SparkApplicationService) (*gin.Engine, error) {

	router := gin.Default()
	router.Use(sgMiddleware.ApplicationErrorHandler)

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

	// Versioned routes
	v1Group := router.Group("/api/v1")

	v1.RegisterKubeflowApplicationRoutes(v1Group, sgConf, appService)

	return router, nil

}
