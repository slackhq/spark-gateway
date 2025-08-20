package health

import "github.com/gin-gonic/gin"

func RegisterHealthRoutes(rg *gin.RouterGroup) {

	h := &GatewayHealthHandler{}

	rg.GET("/healthz", h.Health)
}
