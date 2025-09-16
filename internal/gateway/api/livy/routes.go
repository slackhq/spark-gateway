package livy

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
)

// RegisterLivyBatchRoutes registers routes handling GatewayApplication submissions
func RegisterLivyBatchRoutes(rg *gin.RouterGroup, livyService service.LivyApplicationService) {

	h := NewLivyBatchApplicationHandler(livyService)

	rg.GET("/batches", h.List)
	rg.POST("/batches", h.Create)

	rg.GET("/batches/:batchId", h.Get)
	rg.GET("/batches/:batchId/state", h.State)
	rg.DELETE("/batches/:batchId", h.Delete)

	rg.GET("/batches/:batchId/log", h.Logs)

}
