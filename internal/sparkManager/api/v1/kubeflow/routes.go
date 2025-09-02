package v1kubeflow

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/sparkManager/service"
)

// RegisterKubeflowApplicationRoutes registers routes handling Kubeflow SparkOperator SparkApplication submissions
func RegisterKubeflowApplicationRoutes(rg *gin.RouterGroup, sgConf *config.SparkGatewayConfig, appService service.SparkApplicationService) {

	h := NewSparkApplicationHandler(appService, sgConf.DefaultLogLines)

	rg.GET("/:namespace", h.List)

	rg.POST("/:namespace/:name", h.Create)
	rg.GET("/:namespace/:name", h.Get)
	rg.GET("/:namespace/:name/status", h.Status)
	rg.GET("/:namespace/:name/logs", h.Logs)

	rg.DELETE("/:namespace/:name", h.Delete)

}
