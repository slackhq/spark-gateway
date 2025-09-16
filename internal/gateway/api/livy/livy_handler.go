package livy

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

type LivyHandler struct {
	livyService service.LivyApplicationService
}

func NewLivyBatchApplicationHandler(livyService service.LivyApplicationService) *LivyHandler {
	return &LivyHandler{
		livyService: livyService,
	}
}

func (l *LivyHandler) Get(c *gin.Context) {

	getId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	gotBatch, err := l.livyService.Get(c, getId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gotBatch)

}

func (l *LivyHandler) List(c *gin.Context) {

	var err error

	var from int
	fromParam := c.Query("from")
	if fromParam != "" {
		from, err = strconv.Atoi(fromParam)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "from must be an int"})
		}
	}

	var size int
	sizeParam := c.Query("size")
	if sizeParam != "" {
		size, err = strconv.Atoi(sizeParam)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "size must be an int"})
		}
	}

	listBatches, err := l.livyService.List(c, from, size)
	if err != nil {
		c.Error(fmt.Errorf("error listing Livy SparkApplications: %w", err))
	}

	c.JSON(http.StatusCreated, domain.LivyListBatchesResponse{
		From:     from,
		Total:    len(listBatches),
		Sessions: listBatches,
	})

}

func (l *LivyHandler) Create(c *gin.Context) {

	var createReq domain.LivyCreateBatchRequest
	if err := c.ShouldBindJSON(&createReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": err.Error()})
	}

	// Check for doAs as that takes priority over all
	doAs := c.Query("doAs")
	if doAs != "" {
		createReq.ProxyUser = doAs
	}

	// If no doAs, check proxyUser from request, or finally set
	// batch to submitting user
	if createReq.ProxyUser == "" {
		gotUser, exists := c.Get("user")
		if !exists {
			c.Error(errors.New("no user set, congratulations you've encountered a bug that should never happen"))
			return
		}
		createReq.ProxyUser = gotUser.(string)
	}

	// Get namespace from headers
	namespace := c.GetHeader("X-Spark-Gateway-Livy-Namespace")
	if namespace == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "spark gateway livy API requires X-Spark-Gateway-Livy-Namespace' header"})
	}

	createdBatch, err := l.livyService.Create(c, createReq, namespace)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, createdBatch)

}
func (l *LivyHandler) Delete(c *gin.Context) {
	deleteId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	err = l.livyService.Delete(c, deleteId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}

func (l *LivyHandler) Logs(c *gin.Context) {
	logsId, err := strconv.Atoi(c.Param("batchId"))

	var from int
	fromParam := c.Query("from")
	if fromParam != "" {
		from, err = strconv.Atoi(fromParam)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "from must be an int"})
		}
	}

	var size int
	sizeParam := c.Query("size")
	if sizeParam != "" {
		size, err = strconv.Atoi(sizeParam)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "size must be an int"})
		}
	}

	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	logs, err := l.livyService.Logs(c, logsId, from, size)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, domain.LivyLogBatchResponse{
		Id:   logsId,
		From: from,
		Size: size,
		Log:  logs,
	})
}

func (l *LivyHandler) State(c *gin.Context) {

	getId, err := strconv.Atoi(c.Param("batchId"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": "batchId must be an int"})
		return
	}

	gotBatch, err := l.livyService.Get(c, getId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, domain.LivyGetBatchStateResponse{
		Id:    getId,
		State: gotBatch.State,
	})

}

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
