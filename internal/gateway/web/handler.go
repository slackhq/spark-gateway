package web

import (
	"net/http"

	"github.com/a-h/templ/examples/integration-gin/gintemplrenderer"
	"github.com/gin-gonic/gin"

	"github.com/slackhq/spark-gateway/internal/gateway/cluster"
	"github.com/slackhq/spark-gateway/internal/gateway/web/app"
)

type WebHandler struct {
	localClusterRepo *cluster.LocalClusterRepo
	engine           *gin.Engine
	routerGroup      *gin.RouterGroup
}

func NewWebHandler(localClusterRepo *cluster.LocalClusterRepo, engine *gin.Engine, routerGroup *gin.RouterGroup) *WebHandler {
	return &WebHandler{
		localClusterRepo: localClusterRepo,
		engine:           engine,
		routerGroup:      routerGroup,
	}
}

func (h *WebHandler) RegisterRoutes() {
	uiGroup := h.routerGroup.Group("/ui")

	ginHtmlRenderer := h.engine.HTMLRender
	h.engine.HTMLRender = &gintemplrenderer.HTMLTemplRenderer{FallbackHtmlRenderer: ginHtmlRenderer}

	uiGroup.GET("/", h.main)
	uiGroup.GET("/clusters", h.clusters)

}

func (h *WebHandler) main(c *gin.Context) {
	r := gintemplrenderer.New(c, http.StatusOK, app.Main())
	c.Render(http.StatusOK, r)
}
