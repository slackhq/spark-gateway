package health

import "github.com/gin-gonic/gin"

func RegisterHealthRoutes(rg *gin.RouterGroup) {

	h := &HealthHandler{}

	rg.GET("/health", h.Health)

}
