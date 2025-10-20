package livy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/gateway/service"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	"github.com/slackhq/spark-gateway/internal/shared/middleware"
	"github.com/stretchr/testify/assert"
)

var testConfig = &config.SparkGatewayConfig{DefaultLogLines: 100}

func init() {
	gin.SetMode(gin.TestMode)

}

func NewLivyRouter() (*gin.Engine, *gin.RouterGroup) {
	router := gin.Default()
	v1Group := router.Group("/api/livy")
	v1Group.Use(middleware.ApplicationErrorHandler)

	return router, v1Group
}

var errorHandlerTests = []struct {
	test       string
	err        error
	returnJSON string
	statusCode int
}{
	{
		test:       "app not found err",
		err:        gatewayerrors.NewNotFound(errors.New("error getting Livy GatewayApplication")),
		returnJSON: `{"msg":"error getting Livy GatewayApplication"}`,
		statusCode: http.StatusNotFound,
	},
	{
		test:       "already exists",
		err:        gatewayerrors.NewAlreadyExists(errors.New("resource.group \"test\" already exists")),
		returnJSON: `{"msg":"resource.group \"test\" already exists"}`,
		statusCode: http.StatusConflict,
	},
	{
		test:       "internal server error",
		err:        errors.New("test error"),
		returnJSON: `{"msg":"test error"}`,
		statusCode: http.StatusInternalServerError,
	},
}

func TestApplicationHandlerErrorHandler(t *testing.T) {

	for _, test := range errorHandlerTests {
		t.Run(test.test, func(t *testing.T) {
			router := gin.New()
			router.Use(LivyErrorHandler)
			router.Use(func(ctx *gin.Context) {
				ctx.Error(test.err)
			})
			router.GET("/", func(ctx *gin.Context) {})
			req, _ := http.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			responseData, _ := io.ReadAll(w.Body)
			assert.Equal(t, test.statusCode, w.Code, "codes should match")
			assert.Equal(t, test.returnJSON, string(responseData), "returned JSON should match")

		})
	}
}

func TestLivyApplicationhandlerGet(t *testing.T) {
	retApp := &domain.LivyBatch{
		Id:    0,
		AppId: "appId",
		AppInfo: map[string]string{
			"Cluster":   "cluster",
			"GatewayId": "clusterid-nsid-uuid",
		},
		TTL:   "1",
		Log:   []string{},
		State: domain.LivySessionStateBusy.String(),
	}

	service := &service.LivyApplicationServiceMock{
		GetFunc: func(ctx context.Context, batchId int) (*domain.LivyBatch, error) {
			return retApp, nil
		},
	}

	router, livyGroup := NewLivyRouter()
	RegisterLivyBatchRoutes(livyGroup, service)

	req, _ := http.NewRequest("GET", "/api/livy/batches/0", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.LivyBatch
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusOK, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}

func TestLivyApplicationhandlerGetBadIdInt(t *testing.T) {
	service := &service.LivyApplicationServiceMock{}

	router, livyGroup := NewLivyRouter()
	RegisterLivyBatchRoutes(livyGroup, service)

	req, _ := http.NewRequest("GET", "/api/livy/batches/badId", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"msg":"batchId must be an int"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusBadRequest, w.Code, "codes must match")
	assert.Equal(t, resp, string(responseData), "error msg should match")
}
func TestLivyApplicationhandlerGetBadIdNegativeNumber(t *testing.T) {
	service := &service.LivyApplicationServiceMock{}

	router, livyGroup := NewLivyRouter()
	RegisterLivyBatchRoutes(livyGroup, service)

	req, _ := http.NewRequest("GET", "/api/livy/batches/-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := `{"msg":"batchId must be greater than or equal to 0"}`

	responseData, _ := io.ReadAll(w.Body)
	assert.Equal(t, http.StatusBadRequest, w.Code, "codes must match")
	assert.Equal(t, resp, string(responseData), "batchId ust be an int", "error msg should match")
}

func TestLivyApplicationHandlerCreate(t *testing.T) {
	router, livyGroup := NewLivyRouter()

	livyGroup.Use(func(ctx *gin.Context) {
		ctx.Set("user", "user")
		ctx.Next()
	})

	retApp := &domain.LivyBatch{
		Id:    0,
		AppId: "appId",
		AppInfo: map[string]string{
			"Cluster":   "cluster",
			"GatewayId": "clusterid-nsid-uuid",
		},
		TTL:   "1",
		Log:   []string{},
		State: domain.LivySessionStateBusy.String(),
	}

	service := &service.LivyApplicationServiceMock{
		CreateFunc: func(ctx context.Context, createReq domain.LivyCreateBatchRequest, namespace string) (*domain.LivyBatch, error) {
			return retApp, nil
		},
	}

	RegisterLivyBatchRoutes(livyGroup, service)

	createReq := domain.LivyCreateBatchRequest{
		File:      "testFile",
		ProxyUser: "user",
		ClassName: "className",
		Args: []string{
			"arg1",
		},
		Jars: []string{
			"jar1",
		},
		PyFiles: []string{
			"pyFile1",
		},
		Files: []string{
			"file1",
		},
		DriverMemory:   "1G",
		DriverCores:    1,
		ExecutorMemory: "1G",
		ExecutorCores:  1,
		NumExecutors:   10,
		Archives: []string{
			"archive1",
		},
		Queue: "queue",
		Name:  "name",
		Conf: domain.LivyConf{
			"conf1": "val1",
		},
	}

	jsonReq, _ := json.Marshal(createReq)
	req, _ := http.NewRequest("POST", "/api/livy/batches", bytes.NewBuffer(jsonReq))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var gotApp domain.LivyBatch
	json.Unmarshal(w.Body.Bytes(), &gotApp)

	assert.Equal(t, http.StatusCreated, w.Code, "codes should match")
	assert.Equal(t, gotApp, *retApp, "returned JSON should match")
}

func TestResolveProxyUser_ExistingProxyUser(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest("POST", "/test", nil)
	c.Request = req

	createReq := &domain.LivyCreateBatchRequest{
		ProxyUser: "requestUser",
	}

	// Test
	err := resolveProxyUser(c, createReq)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "requestUser", createReq.ProxyUser, "existing proxyUser should be preserved")
}

func TestResolveProxyUser_AuthenticatedUser(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest("POST", "/test", nil)
	c.Request = req
	c.Set("user", "authenticatedUser")

	createReq := &domain.LivyCreateBatchRequest{
		ProxyUser: "", // Empty proxy user
	}

	// Test
	err := resolveProxyUser(c, createReq)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "authenticatedUser", createReq.ProxyUser, "should fall back to authenticated user")
}

func TestResolveProxyUser_NoUserContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest("POST", "/test", nil)
	c.Request = req
	// Don't set user context

	createReq := &domain.LivyCreateBatchRequest{
		ProxyUser: "", // Empty proxy user
	}

	// Test
	err := resolveProxyUser(c, createReq)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no user set")
}

