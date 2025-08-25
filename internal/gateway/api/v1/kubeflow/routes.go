package v1kubeflow

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
)

// RegisterKubeflowApplicationRoutes registers routes handling Kubeflow SparkOperator SparkApplication submissions
func RegisterKubeflowApplicationRoutes(rg *gin.RouterGroup, sgConf *config.SparkGatewayConfig, appService service.SparkApplicationService) {

	h := NewKubeflowApplicationHandler(appService, sgConf.DefaultLogLines)

	rg.GET("/applications", h.List)
	rg.POST("/applications", h.Create)

	rg.GET("/applications/:gatewayId", h.Get)
	rg.DELETE("/applications/:gatewayId", h.Delete)

	rg.GET("/applications/:gatewayId/status", h.Status)
	rg.GET("/applications/:gatewayId/logs", h.Logs)

}
