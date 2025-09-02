package web

import (
	"net/http"

	"github.com/a-h/templ/examples/integration-gin/gintemplrenderer"
	"github.com/gin-gonic/gin"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"sigs.k8s.io/yaml"

	"github.com/slackhq/spark-gateway/internal/gateway/application/handler"
	"github.com/slackhq/spark-gateway/internal/gateway/cluster"
	"github.com/slackhq/spark-gateway/internal/gateway/web/app"
	"github.com/slackhq/spark-gateway/pkg/model"
)

type WebHandler struct {
	localClusterRepo          *cluster.LocalClusterRepo
	gatewayApplicationService handler.GatewayApplicationService
	engine                    *gin.Engine
	routerGroup               *gin.RouterGroup
}

func NewWebHandler(localClusterRepo *cluster.LocalClusterRepo, gatewayApplicationService handler.GatewayApplicationService, engine *gin.Engine, routerGroup *gin.RouterGroup) *WebHandler {
	return &WebHandler{
		localClusterRepo:          localClusterRepo,
		gatewayApplicationService: gatewayApplicationService,
		engine:                    engine,
		routerGroup:               routerGroup,
	}
}

func (h *WebHandler) RegisterRoutes() {
	uiGroup := h.routerGroup.Group("/ui")

	ginHtmlRenderer := h.engine.HTMLRender
	h.engine.HTMLRender = &gintemplrenderer.HTMLTemplRenderer{FallbackHtmlRenderer: ginHtmlRenderer}

	uiGroup.GET("/", h.main)
	uiGroup.GET("/clusters", h.clusters)
	uiGroup.GET("/applications/:gatewayId/spec", h.applicationSpec)

}

func (h *WebHandler) main(c *gin.Context) {
	clusters, err := h.localClusterRepo.GetAll()
	if err != nil {
		c.Error(err)
		return
	}

	selectedCluster := c.Query("cluster")
	selectedNamespace := c.Query("namespace")

	var applications []*model.GatewayApplicationMeta
	var namespaces []model.KubeNamespace

	// Get applications if both cluster and namespace are selected
	if selectedCluster != "" && selectedNamespace != "" {
		applications, err = h.gatewayApplicationService.List(c, selectedCluster, selectedNamespace, nil)
		if err != nil {
			c.Error(err)
			return
		}
	}

	// Get namespaces if cluster is selected
	if selectedCluster != "" {
		for _, cluster := range clusters {
			if cluster.Name == selectedCluster {
				namespaces = cluster.Namespaces
				break
			}
		}
	}

	// Get counts of applications in different states across all clusters
	counts := app.ApplicationCounts{}
	if err == nil {
		// Define the states we want to count
		states := map[string]v1beta2.ApplicationStateType{
			"submitted": v1beta2.ApplicationStateSubmitted,
			"running":   v1beta2.ApplicationStateRunning,
			"completed": v1beta2.ApplicationStateCompleted,
			"failed":    v1beta2.ApplicationStateFailed,
		}

		for _, cluster := range clusters {
			for _, namespace := range cluster.Namespaces {
				for stateName, state := range states {
					apps, err := h.gatewayApplicationService.List(c, cluster.Name, namespace.Name, &state)
					if err == nil {
						switch stateName {
						case "submitted":
							counts.Submitted += len(apps)
						case "running":
							counts.Running += len(apps)
						case "completed":
							counts.Completed += len(apps)
						case "failed":
							counts.Failed += len(apps)
						}
					}
				}
			}
		}
	}

	if c.GetHeader("HX-Request") == "true" {
		r := gintemplrenderer.New(c, http.StatusOK, app.MainContent(counts, clusters, applications, selectedCluster, selectedNamespace, namespaces))
		c.Render(http.StatusOK, r)
	} else {
		r := gintemplrenderer.New(c, http.StatusOK, app.Main(counts, clusters, applications, selectedCluster, selectedNamespace, namespaces))
		c.Render(http.StatusOK, r)
	}
}

func (h *WebHandler) clusters(c *gin.Context) {
	clusters, err := h.localClusterRepo.GetAll()
	if err != nil {
		c.Error(err)
		return
	}

	// Check if this is an HTMX request (partial update)
	if c.GetHeader("HX-Request") == "true" {
		r := gintemplrenderer.New(c, http.StatusOK, app.ClustersContent(clusters))
		c.Render(http.StatusOK, r)
	} else {
		// Full page load
		r := gintemplrenderer.New(c, http.StatusOK, app.Clusters(clusters))
		c.Render(http.StatusOK, r)
	}
}

func (h *WebHandler) applicationSpec(c *gin.Context) {
	gatewayId := c.Param("gatewayId")
	if gatewayId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "gatewayId is required"})
		return
	}

	// Get the full application data using the service
	gatewayApp, err := h.gatewayApplicationService.Get(c, gatewayId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Convert SparkApplication to YAML
	yamlData, err := yaml.Marshal(gatewayApp.SparkApplication)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to marshal application to YAML"})
		return
	}

	// Return YAML spec only
	response := gin.H{
		"spec": string(yamlData),
	}

	c.JSON(http.StatusOK, response)
}
