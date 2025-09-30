package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/database"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

// wrapLivyError wraps an error with a Livy-specific message and returns a GatewayError
func wrapLivyError(err error, message string) error {
	return gatewayerrors.NewFrom(fmt.Errorf("%s: %w", message, err))
}

//go:generate moq -rm  -out mocklivyapplicationservice.go . LivyApplicationService

type LivyApplicationService interface {
	Get(ctx context.Context, batchId int) (*domain.LivyBatch, error)
	List(ctx context.Context, from int, size int) ([]*domain.LivyBatch, error)
	Create(ctx context.Context, createReq domain.LivyCreateBatchRequest, namespace string) (*domain.LivyBatch, error)
	Delete(ctx context.Context, batchId int) error
	Logs(ctx context.Context, batchId int, size int) ([]string, error)
}

type livyService struct {
	appService GatewayApplicationService
	database   database.LivyApplicationDatabase
	namespace  string
}

// getLivyAppByBatchId retrieves a LivyApplication from the database by batchId
func (l *livyService) getLivyAppByBatchId(ctx context.Context, batchId int) (*database.LivyApplication, error) {
	livyApp, err := l.database.GetByBatchId(ctx, batchId)
	if err != nil {
		return nil, wrapLivyError(err, "error getting GatewayApplication from Livy BatchId")
	}
	return &livyApp, nil
}

func NewLivyService(appService GatewayApplicationService, database database.LivyApplicationDatabase, namespace string) *livyService {
	return &livyService{
		appService: appService,
		database:   database,
		namespace:  namespace,
	}
}

func (l *livyService) Get(ctx context.Context, batchId int) (*domain.LivyBatch, error) {

	livyApp, err := l.getLivyAppByBatchId(ctx, batchId)
	if err != nil {
		return nil, err
	}

	gotApp, err := l.appService.Get(ctx, livyApp.GatewayID)
	if err != nil {
		return nil, wrapLivyError(err, "error getting GatewayApplication")
	}

	return gotApp.ToLivyBatch(int32(livyApp.BatchID)), nil
}

func (l *livyService) List(ctx context.Context, from int, size int) ([]*domain.LivyBatch, error) {

	livyApps, err := l.database.ListFrom(ctx, from, size)
	if err != nil {
		return nil, wrapLivyError(err, "error listing Livy GatewayApplications")
	}

	var retApps []*domain.LivyBatch
	for _, livyApp := range livyApps {
		gotApp, err := l.appService.Get(ctx, livyApp.GatewayID)
		if err != nil {
			return nil, wrapLivyError(err, "error listing Livy GatewayApplications")
		}

		livyBatch := gotApp.ToLivyBatch(int32(livyApp.BatchID))
		retApps = append(retApps, livyBatch)
	}

	return retApps, nil

}

func (l *livyService) Create(ctx context.Context, createReq domain.LivyCreateBatchRequest, namespace string) (*domain.LivyBatch, error) {
	// Determine the target namespace
	ns := namespace
	if ns == "" {
		ns = l.namespace
	}

	// Convert Livy request to SparkApplication
	application := createReq.ToV1Beta2SparkApplication(ns)

	// Create the SparkApplication in Kubernetes
	gatewayApp, err := l.appService.Create(ctx, application, *application.Spec.ProxyUser)
	if err != nil {
		return nil, wrapLivyError(err, "error creating Livy GatewayApplication")
	}

	// Track the application in the database
	livyApp, err := l.database.InsertLivyApplication(ctx, gatewayApp.GatewayId)
	if err != nil {
		// Cleanup the K8s resource on database failure
		if deleteErr := l.appService.Delete(ctx, gatewayApp.GatewayId); deleteErr != nil {
			return nil, wrapLivyError(err, fmt.Sprintf("error tracking Livy application '%s' and failed cleanup", gatewayApp.GatewayId))
		}
		return nil, wrapLivyError(err, fmt.Sprintf("error tracking Livy application '%s' in database", gatewayApp.GatewayId))
	}

	return gatewayApp.ToLivyBatch(int32(livyApp.BatchID)), nil
}

func (l *livyService) Delete(ctx context.Context, batchId int) error {

	livyApp, err := l.getLivyAppByBatchId(ctx, batchId)
	if err != nil {
		return err
	}

	if err := l.appService.Delete(ctx, livyApp.GatewayID); err != nil {
		return wrapLivyError(err, "error deleting Livy GatewayApplication")
	}

	return nil
}

func (l *livyService) Logs(ctx context.Context, batchId int, size int) ([]string, error) {
	livyApp, err := l.getLivyAppByBatchId(ctx, batchId)
	if err != nil {
		return nil, err
	}

	logs, err := l.appService.Logs(ctx, livyApp.GatewayID, size)
	if err != nil {
		return nil, wrapLivyError(err, "error getting logs for Livy GatewayApplication")
	}

	if logs == nil {
		return []string{}, nil
	}

	logSplice := strings.Split(*logs, "\n")
	return logSplice, nil

}
