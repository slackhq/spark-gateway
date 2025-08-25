package swagger

import (
	"net/http"

	"github.com/gin-gonic/gin"
	swaggerDocs "github.com/slackhq/spark-gateway/docs/swagger"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func RegisterSwaggerRoutes(rg *gin.RouterGroup) {
	swaggerDocs.SwaggerInfo.BasePath = "/api"

	// Swagger UI on /swagger/index.html
	rg.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, ginSwagger.DefaultModelsExpandDepth(-1)))
	// Redirect /doc and /docs/ to /swagger/index.html
	rg.GET("/docs", func(ctx *gin.Context) {
		ctx.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})

}
