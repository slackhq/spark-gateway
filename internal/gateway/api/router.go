package api

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/api/health"
	v1kubeflow "github.com/slackhq/spark-gateway/internal/gateway/api/v1/kubeflow"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	sgHttp "github.com/slackhq/spark-gateway/internal/shared/http"
)

func NewRouter(sgConf *config.SparkGatewayConfig, appService service.GatewayApplicationService) *gin.Engine {

	router := gin.Default()
	router.Use(sgHttp.ApplicationErrorHandler)

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

	// Versioned routes

	v1Group := router.Group("/v1")

	v1kubeflow.RegisterKubeflowApplicationRoutes(v1Group, sgConf, appService)

	return router

}
