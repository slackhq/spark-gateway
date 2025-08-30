package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
)

// RegisterApplicationRoutes registers routes handling GatewayApplication submissions
func RegisterGatewayApplicationRoutes(rg *gin.RouterGroup, sgConf *config.SparkGatewayConfig, appService service.GatewayApplicationService) {

	h := NewGatewayApplicationHandler(appService, sgConf.DefaultLogLines)

	rg.GET("/applications", h.List)
	rg.POST("/applications", h.Create)

	rg.GET("/applications/:gatewayId", h.Get)
	rg.DELETE("/applications/:gatewayId", h.Delete)

	rg.GET("/applications/:gatewayId/status", h.Status)
	rg.GET("/applications/:gatewayId/logs", h.Logs)

}
