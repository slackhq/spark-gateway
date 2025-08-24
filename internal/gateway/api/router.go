package api

import (
	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/api/health"
	v1kubeflow "github.com/slackhq/spark-gateway/internal/gateway/api/v1/kubeflow"
)

func NewRouter(userService *service.UserService) {

	router := gin.Default()

	// Root group for unversioned routes
	rootGroup := router.Group("")

	health.RegisterHealthRoutes(rootGroup)

}
