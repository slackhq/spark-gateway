package api

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/api/health"
	"github.com/slackhq/spark-gateway/internal/sparkManager/api/v1/application/service"
)

func NewGatewayRouter(appService *service.SparkApplicationService) *gin.Engine {
	router := gin.Default()

	rootGroup := router.Group("")

	// Register unversioned routes
	health.RegisterHealthRoutes(rootGroup)

	// Versioned routes

	v1Group := router.Group("/api/v1")

	v1.RegisterApplicationRoutes(v1Group, appService)

	

}
