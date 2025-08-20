package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
)

func RegisterApplicationRoutes(rg *gin.RouterGroup, appService *service.GatewayApplicationService) {

	h := NewApplicationHandler(appService, 100)

	rg.GET("/healthz", h.Health)
}
