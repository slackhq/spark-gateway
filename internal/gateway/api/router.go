package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/api/health"
	"github.com/slackhq/spark-gateway/internal/gateway/api/livy"
	"github.com/slackhq/spark-gateway/internal/gateway/api/middleware"
	"github.com/slackhq/spark-gateway/internal/gateway/api/swagger"
	v1 "github.com/slackhq/spark-gateway/internal/gateway/api/v1"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	sgMiddleware "github.com/slackhq/spark-gateway/internal/shared/middleware"
)

func NewRouter(sgConf *config.SparkGatewayConfig, appService service.GatewayApplicationService, livyService service.LivyApplicationService) (*gin.Engine, error) {

	router := gin.Default()

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

	if sgConf.GatewayConfig.EnableSwaggerUI {
		swagger.RegisterSwaggerRoutes(rootGroup)
	}

	// Versioned routes
	v1Group := router.Group("/api/v1")
	v1Group.Use(sgMiddleware.ApplicationErrorHandler)
	if err := middleware.AddMiddleware(sgConf.GatewayConfig.Middleware, v1Group); err != nil {
		return nil, fmt.Errorf("error adding middlewares to routes: %w", err)
	}

	v1.RegisterGatewayApplicationRoutes(v1Group, sgConf, appService)

	if sgConf.LivyConfig.Enable {
		livyGroup := router.Group("/api/livy")
		livyGroup.Use(livy.LivyErrorHandler)
		livy.RegisterLivyBatchRoutes(livyGroup, livyService)
	}

	return router, nil

}
