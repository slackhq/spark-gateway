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

// validateIntParam extracts and validates an integer parameter from URL path or query string
func validateIntParam(c *gin.Context, paramName string, isPathParam bool, required bool) (int, bool) {
	var paramValue string
	if isPathParam {
		paramValue = c.Param(paramName)
	} else {
		paramValue = c.Query(paramName)
		if paramValue == "" && !required {
			return 0, true // Optional query parameter, return success with 0 value
		}
	}

	value, err := strconv.Atoi(paramValue)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": paramName + " must be an int"})
		return 0, false
	}
	if value < 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": paramName + " must be greater than or equal to 0"})
		return 0, false
	}
	return value, true
}

type LivyHandler struct {
	livyService service.LivyApplicationService
}

func NewLivyBatchApplicationHandler(livyService service.LivyApplicationService) *LivyHandler {
	return &LivyHandler{
		livyService: livyService,
	}
}

func (l *LivyHandler) Get(c *gin.Context) {
	getId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
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
	from, ok := validateIntParam(c, "from", false, false)
	if !ok {
		return
	}

	size, ok := validateIntParam(c, "size", false, false)
	if !ok {
		return
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

	// Get namespace from headers if supplied
	namespace := c.GetHeader("X-Spark-Gateway-Livy-Namespace")

	createdBatch, err := l.livyService.Create(c, createReq, namespace)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, createdBatch)

}
func (l *LivyHandler) Delete(c *gin.Context) {
	deleteId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	err := l.livyService.Delete(c, deleteId)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}

func (l *LivyHandler) Logs(c *gin.Context) {
	size, ok := validateIntParam(c, "size", false, false)
	if !ok {
		return
	}

	logsId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
		return
	}

	logs, err := l.livyService.Logs(c, logsId, size)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, domain.LivyLogBatchResponse{
		Id:   logsId,
		From: -1,
		Size: size,
		Log:  logs,
	})
}

func (l *LivyHandler) State(c *gin.Context) {
	getId, ok := validateIntParam(c, "batchId", true, true)
	if !ok {
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
