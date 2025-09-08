package livy

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	"github.com/slackhq/spark-gateway/internal/types/livy"
)

var livyBatchId atomic.Int32

type LivyHandler struct {
	appService service.GatewayApplicationService
	namespace  string
	appCache   sync.Map
}

func NewLivyBatchApplicationHandler(appService service.GatewayApplicationService) *LivyHandler {
	return &LivyHandler{
		appService: appService,
	}
}

func (l *LivyHandler) List(c *gin.Context) {}
func (l *LivyHandler) Create(c *gin.Context) {

	var createReq CreateBatchRequest
	if err := c.ShouldBindJSON(&createReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": err.Error()})
	}

	var user string

	// Check for doAs as that takes priority over all
	doAs := c.Query("doAs")
	if doAs != "" {
		user = doAs
	}

	// If no doAs, check proxyUser from request, or finally set
	// batch to submitting user
	if createReq.ProxyUser == "" {
		gotUser, exists := c.Get("user")
		if !exists {
			c.Error(errors.New("no user set, congratulations you've encountered a bug that should never happen"))
			return
		}
		user = gotUser.(string)
	} else {
		user = createReq.ProxyUser
	}

	// Get next increment of Batch Id
	batchId := livyBatchId.Add(1)

	// Get namespace from headers
	namespace := c.GetHeader("X-Spark-Gateway-Livy-Namespace")
	if namespace == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "spark gateway livy API requires X-Spark-Gateway-Livy-Namespace' header"})
	}

	createdApp, err := l.appService.Create(c, createReq.ToV1Beta2SparkApplication(batchId, namespace), user)

	if err != nil {
		c.Error(err)
		return
	}

	// Store id -> gatewayid for lookups in other
	l.appCache.Store(batchId, createdApp.GatewayId)

	c.JSON(http.StatusCreated, createdApp.SparkApplication.ToLivyBatch(batchId))

}
func (l *LivyHandler) Get(c *gin.Context) {

	getId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	// Get GatewayId from batch id
	livyGatewayId, ok := l.appCache.Load(int32(getId))
	if !ok {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("livy batch '%s' not found", getId)})
		return
	}

	app, err := l.appService.Get(c, livyGatewayId.(string))
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, app.ToLivyBatch(getId))

}
func (l *LivyHandler) State(c *gin.Context) {

	getId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	// Get GatewayId from batch id
	livyGatewayId, ok := l.appCache.Load(int32(getId))
	if !ok {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("livy batch '%s' not found", getId)})
		return
	}

	appStatus, err := l.appService.Status(c, livyGatewayId.(string))
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, appStatus.ToLivyBatchState(getId))

}
func (l *LivyHandler) Delete(c *gin.Context) {
	getId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	// Get GatewayId from batch id
	livyGatewayId, ok := l.appCache.Load(int32(getId))
	if !ok {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("livy batch '%s' not found", getId)})
		return
	}

	err = l.appService.Delete(c, livyGatewayId.(string))
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}
func (l *LivyHandler) Logs(c *gin.Context) {}

// LivyErrorHandler attempts to coerce the last error within gin.Context.Errors.Last to a
// GatewayError for proper HttpStatus attribution. If the error casting fails, it aborts the connection
// with the Error message and a 500 error
func LivyErrorHandler(c *gin.Context) {
	// Before request
	c.Next()
	// After request
	if len(c.Errors) != 0 {
		lastErr := c.Errors.Last()

		var gatewayError gatewayerrors.GatewayError
		if errors.As(lastErr, &gatewayError) {
			c.AbortWithStatusJSON(gatewayError.Status, gin.H{"msg": gatewayError.Error()})
			return
		}

		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"msg": lastErr.Error()})
		return

	}
}
