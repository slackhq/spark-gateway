package service

import (
	"context"
	"errors"
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/database"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
	"github.com/slackhq/spark-gateway/internal/shared/util"
	"github.com/stretchr/testify/assert"
)

func TestLivyService_Get_Success(t *testing.T) {
	ctx := context.Background()
	batchId := 123

	// Mock data
	livyApp := database.LivyApplication{
		BatchID:   int64(batchId),
		GatewayID: "clusterid-nsid-uuid",
	}

	gatewayApp := &domain.GatewayApplication{
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "test-cluster",
		User:      "testuser",
		SparkApplication: domain.GatewaySparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Ptr(int64(3600)),
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-app-123",
			},
		},
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		GetByBatchIdFunc: func(ctx context.Context, batchId int) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		GetFunc: func(ctx context.Context, gatewayId string) (*domain.GatewayApplication, error) {
			return gatewayApp, nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Get(ctx, batchId)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int32(batchId), result.Id)
}

func TestLivyService_Get_DatabaseError(t *testing.T) {
	ctx := context.Background()
	batchId := 123

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		GetByBatchIdFunc: func(ctx context.Context, batchId int) (database.LivyApplication, error) {
			return database.LivyApplication{}, errors.New("database error")
		},
	}

	mockAppService := &GatewayApplicationServiceMock{}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Get(ctx, batchId)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "error getting GatewayApplication from Livy BatchId")
}

func TestLivyService_Create_Success(t *testing.T) {
	ctx := context.Background()

	createReq := domain.LivyCreateBatchRequest{
		File:      "test.jar",
		ProxyUser: "testuser",
		Name:      "test-job",
	}

	gatewayApp := &domain.GatewayApplication{
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "test-cluster",
		User:      "testuser",
		SparkApplication: domain.GatewaySparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Ptr(int64(3600)),
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-app-123",
			},
		},
	}

	livyApp := database.LivyApplication{
		BatchID:   456,
		GatewayID: "clusterid-nsid-uuid",
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		InsertLivyApplicationFunc: func(ctx context.Context, gatewayId string) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, proxyUser string) (*domain.GatewayApplication, error) {
			// Verify the application was created correctly
			assert.Equal(t, "test-job", application.Name)
			assert.Equal(t, "default", application.Namespace)
			assert.Equal(t, "testuser", *application.Spec.ProxyUser)
			assert.Equal(t, domain.DEFAULT_SPARK_MODE, application.Spec.Mode)
			assert.Equal(t, domain.DEFAULT_SPARK_VERSION, application.Spec.SparkVersion)
			return gatewayApp, nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Create(ctx, createReq, "")

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int32(456), result.Id)
}

func TestLivyService_Create_DatabaseError_WithCleanup(t *testing.T) {
	ctx := context.Background()

	createReq := domain.LivyCreateBatchRequest{
		File:      "test.jar",
		ProxyUser: "testuser",
		Name:      "test-job",
	}

	gatewayApp := &domain.GatewayApplication{
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "test-cluster",
		User:      "testuser",
		SparkApplication: domain.GatewaySparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Ptr(int64(3600)),
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-app-123",
			},
		},
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		InsertLivyApplicationFunc: func(ctx context.Context, gatewayId string) (database.LivyApplication, error) {
			return database.LivyApplication{}, errors.New("database error")
		},
	}

	cleanupCalled := false
	mockAppService := &GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, proxyUser string) (*domain.GatewayApplication, error) {
			return gatewayApp, nil
		},
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			cleanupCalled = true
			assert.Equal(t, "clusterid-nsid-uuid", gatewayId)
			return nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Create(ctx, createReq, "")

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, cleanupCalled, "cleanup should have been called")
	assert.Contains(t, err.Error(), "error tracking Livy application 'clusterid-nsid-uuid' in database")
}

func TestLivyService_Delete_Success(t *testing.T) {
	ctx := context.Background()
	batchId := 123

	livyApp := database.LivyApplication{
		BatchID:   int64(batchId),
		GatewayID: "clusterid-nsid-uuid",
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		GetByBatchIdFunc: func(ctx context.Context, batchId int) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		DeleteFunc: func(ctx context.Context, gatewayId string) error {
			assert.Equal(t, "clusterid-nsid-uuid", gatewayId)
			return nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	err := service.Delete(ctx, batchId)

	// Assertions
	assert.NoError(t, err)
}

func TestLivyService_Logs_Success(t *testing.T) {
	ctx := context.Background()
	batchId := 123
	size := 100

	livyApp := database.LivyApplication{
		BatchID:   int64(batchId),
		GatewayID: "clusterid-nsid-uuid",
	}

	logContent := "log line 1\nlog line 2\nlog line 3"
	expectedLogs := []string{"log line 1", "log line 2", "log line 3"}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		GetByBatchIdFunc: func(ctx context.Context, batchId int) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		LogsFunc: func(ctx context.Context, gatewayId string, size int) (*string, error) {
			assert.Equal(t, "clusterid-nsid-uuid", gatewayId)
			assert.Equal(t, 100, size)
			return &logContent, nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Logs(ctx, batchId, size)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, expectedLogs, result)
}

func TestLivyService_Logs_NilLogs(t *testing.T) {
	ctx := context.Background()
	batchId := 123
	size := 100

	livyApp := database.LivyApplication{
		BatchID:   int64(batchId),
		GatewayID: "clusterid-nsid-uuid",
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		GetByBatchIdFunc: func(ctx context.Context, batchId int) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		LogsFunc: func(ctx context.Context, gatewayId string, size int) (*string, error) {
			return nil, nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Logs(ctx, batchId, size)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, []string{}, result)
}

func TestWrapLivyError(t *testing.T) {
	originalErr := errors.New("original error")
	message := "test message"

	result := wrapLivyError(originalErr, message)

	// Should be a GatewayError
	var gatewayErr gatewayerrors.GatewayError
	assert.True(t, errors.As(result, &gatewayErr))
	assert.Contains(t, result.Error(), message)
	assert.Contains(t, result.Error(), "original error")
}

func TestLivyService_NamespaceResolution(t *testing.T) {
	ctx := context.Background()

	createReq := domain.LivyCreateBatchRequest{
		File:      "test.jar",
		ProxyUser: "testuser",
		Name:      "test-job",
	}

	gatewayApp := &domain.GatewayApplication{
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "test-cluster",
		User:      "testuser",
		SparkApplication: domain.GatewaySparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Ptr(int64(3600)),
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-app-123",
			},
		},
	}

	livyApp := database.LivyApplication{
		BatchID:   456,
		GatewayID: "clusterid-nsid-uuid",
	}

	tests := []struct {
		name              string
		serviceNamespace  string
		requestNamespace  string
		expectedNamespace string
	}{
		{
			name:              "use request namespace when provided",
			serviceNamespace:  "default",
			requestNamespace:  "custom",
			expectedNamespace: "custom",
		},
		{
			name:              "use service namespace when request is empty",
			serviceNamespace:  "default",
			requestNamespace:  "",
			expectedNamespace: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			mockDatabase := &database.LivyApplicationDatabaseMock{
				InsertLivyApplicationFunc: func(ctx context.Context, gatewayId string) (database.LivyApplication, error) {
					return livyApp, nil
				},
			}

			mockAppService := &GatewayApplicationServiceMock{
				CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, proxyUser string) (*domain.GatewayApplication, error) {
					assert.Equal(t, tt.expectedNamespace, application.Namespace)
					return gatewayApp, nil
				},
			}

			// Create service
			service := NewLivyService(mockAppService, mockDatabase, tt.serviceNamespace)

			// Test
			_, err := service.Create(ctx, createReq, tt.requestNamespace)

			// Assertions
			assert.NoError(t, err)
		})
	}
}

func TestLivyService_Create_FullMapping(t *testing.T) {
	ctx := context.Background()

	// Create a fully populated Livy request with all possible fields
	createReq := domain.LivyCreateBatchRequest{
		File:           "s3://bucket/spark-job.jar",
		ProxyUser:      "dataeng-user",
		ClassName:      "com.company.SparkJob",
		Args:           []string{"--input", "/data/input", "--output", "/data/output", "--mode", "production"},
		Jars:           []string{"s3://bucket/lib1.jar", "s3://bucket/lib2.jar", "hdfs://cluster/shared/common.jar"},
		PyFiles:        []string{"s3://bucket/utils.py", "s3://bucket/helpers.py"},
		Files:          []string{"s3://bucket/config.properties", "s3://bucket/schema.json"},
		DriverMemory:   "4g",
		DriverCores:    2,
		ExecutorMemory: "8g",
		ExecutorCores:  4,
		NumExecutors:   20,
		Archives:       []string{"s3://bucket/dependencies.zip", "hdfs://cluster/data.tar.gz"},
		Queue:          "production",
		Name:           "complex-spark-job",
		Conf: map[string]string{
			"spark.sql.adaptive.enabled":                    "true",
			"spark.sql.adaptive.coalescePartitions.enabled": "true",
			"spark.sql.parquet.compression.codec":           "snappy",
			"spark.serializer":                              "org.apache.spark.serializer.KryoSerializer",
			"spark.dynamicAllocation.enabled":               "true",
			"spark.dynamicAllocation.minExecutors":          "5",
			"spark.dynamicAllocation.maxExecutors":          "50",
		},
	}

	gatewayApp := &domain.GatewayApplication{
		GatewayId: "clusterid-nsid-uuid",
		Cluster:   "production-cluster",
		User:      "dataeng-user",
		SparkApplication: domain.GatewaySparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Ptr(int64(7200)),
			},
			Status: v1beta2.SparkApplicationStatus{
				SparkApplicationID: "spark-app-complex-job",
			},
		},
	}

	livyApp := database.LivyApplication{
		BatchID:   789,
		GatewayID: "clusterid-nsid-uuid",
	}

	// Setup mocks
	mockDatabase := &database.LivyApplicationDatabaseMock{
		InsertLivyApplicationFunc: func(ctx context.Context, gatewayId string) (database.LivyApplication, error) {
			return livyApp, nil
		},
	}

	mockAppService := &GatewayApplicationServiceMock{
		CreateFunc: func(ctx context.Context, application *v1beta2.SparkApplication, proxyUser string) (*domain.GatewayApplication, error) {
			// Verify complete mapping of all fields
			assert.Equal(t, "complex-spark-job", application.Name)
			assert.Equal(t, "custom-namespace", application.Namespace)
			assert.Equal(t, "dataeng-user", proxyUser)
			assert.Equal(t, "dataeng-user", *application.Spec.ProxyUser)

			// Verify application type detection (Java for .jar file)
			assert.Equal(t, v1beta2.SparkApplicationTypeJava, application.Spec.Type)

			// Verify main class and file
			assert.Equal(t, "com.company.SparkJob", *application.Spec.MainClass)
			assert.Equal(t, "s3://bucket/spark-job.jar", *application.Spec.MainApplicationFile)

			// Verify arguments
			expectedArgs := []string{"--input", "/data/input", "--output", "/data/output", "--mode", "production"}
			assert.Equal(t, expectedArgs, application.Spec.Arguments)

			// Verify driver configuration
			assert.Equal(t, int32(2), *application.Spec.Driver.Cores)
			assert.Equal(t, "2", *application.Spec.Driver.CoreLimit)
			assert.Equal(t, "4g", *application.Spec.Driver.Memory)
			assert.Equal(t, "4g", *application.Spec.Driver.MemoryLimit)

			// Verify executor configuration
			assert.Equal(t, int32(4), *application.Spec.Executor.Cores)
			assert.Equal(t, "4", *application.Spec.Executor.CoreLimit)
			assert.Equal(t, "8g", *application.Spec.Executor.Memory)
			assert.Equal(t, "8g", *application.Spec.Executor.MemoryLimit)
			assert.Equal(t, int32(20), *application.Spec.Executor.Instances)

			// Verify dependencies
			expectedJars := []string{"s3://bucket/lib1.jar", "s3://bucket/lib2.jar", "hdfs://cluster/shared/common.jar"}
			assert.Equal(t, expectedJars, application.Spec.Deps.Jars)

			expectedPyFiles := []string{"s3://bucket/utils.py", "s3://bucket/helpers.py"}
			assert.Equal(t, expectedPyFiles, application.Spec.Deps.PyFiles)

			expectedFiles := []string{"s3://bucket/config.properties", "s3://bucket/schema.json"}
			assert.Equal(t, expectedFiles, application.Spec.Deps.Files)

			expectedArchives := []string{"s3://bucket/dependencies.zip", "hdfs://cluster/data.tar.gz"}
			assert.Equal(t, expectedArchives, application.Spec.Deps.Archives)

			// Verify Spark configuration
			expectedConf := map[string]string{
				"spark.sql.adaptive.enabled":                    "true",
				"spark.sql.adaptive.coalescePartitions.enabled": "true",
				"spark.sql.parquet.compression.codec":           "snappy",
				"spark.serializer":                              "org.apache.spark.serializer.KryoSerializer",
				"spark.dynamicAllocation.enabled":               "true",
				"spark.dynamicAllocation.minExecutors":          "5",
				"spark.dynamicAllocation.maxExecutors":          "50",
			}
			assert.Equal(t, expectedConf, application.Spec.SparkConf)

			// Verify defaults
			assert.Equal(t, domain.DEFAULT_SPARK_MODE, application.Spec.Mode)
			assert.Equal(t, domain.DEFAULT_SPARK_VERSION, application.Spec.SparkVersion)

			return gatewayApp, nil
		},
	}

	// Create service
	service := NewLivyService(mockAppService, mockDatabase, "default")

	// Test
	result, err := service.Create(ctx, createReq, "custom-namespace")

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int32(789), result.Id)
	assert.Equal(t, "spark-app-complex-job", result.AppId)
}