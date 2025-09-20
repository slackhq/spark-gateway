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

type LivyApplicationService interface {
	Get(ctx context.Context, batchId int) (*domain.LivyBatch, error)
	List(ctx context.Context, from int, size int) ([]*domain.LivyBatch, error)
	Create(ctx context.Context, application v1beta2.SparkApplication) (*domain.LivyBatch, error)
	Delete(ctx context.Context, batchId int) error
	Logs(ctx context.Context, batchId int, size int) ([]string, error)
}

type livyService struct {
	appService GatewayApplicationService
	database   database.LivyApplicationDatabaseRepository
}

func NewLivyService(appService GatewayApplicationService, database database.LivyApplicationDatabaseRepository) *livyService {
	return &livyService{
		appService: appService,
		database:   database,
	}
}

func (l *livyService) Get(ctx context.Context, batchId int) (*domain.LivyBatch, error) {

	livyApp, err := l.database.GetByBatchId(ctx, batchId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting GatewayApplication from Livy BatchId: %w", err))
	}

	gotApp, err := l.appService.Get(ctx, livyApp.GatewayID)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting GatewayApplication: %w", err))
	}

	return gotApp.ToLivyBatch(int32(livyApp.BatchID)), nil
}

func (l *livyService) List(ctx context.Context, from int, size int) ([]*domain.LivyBatch, error) {

	livyApps, err := l.database.ListFrom(ctx, from, size)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error listing Livy GatewayApplications: %w", err))
	}

	var retApps []*domain.LivyBatch
	for _, livyApp := range livyApps {
		gotApp, err := l.appService.Get(ctx, livyApp.GatewayID)
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error listing Livy GatewayAppications: %w", err))
		}

		livyBatch := gotApp.ToLivyBatch(int32(livyApp.BatchID))
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error converting GatewayAppication to Livy Batch: %w", err))
		}
		retApps = append(retApps, livyBatch)
	}

	return retApps, nil

}

func (l *livyService) Create(ctx context.Context, application v1beta2.SparkApplication) (*domain.LivyBatch, error) {

	gatewayApp, err := l.appService.Create(ctx, &application, *application.Spec.ProxyUser)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating Livy GatewayApplication: %w", err))
	}

	livyApp, err := l.database.InsertLivyApplication(ctx, gatewayApp.GatewayId)
	// If there is an erro saving to db, we need to delete the app from running
	if err != nil {
		if deleteErr := l.appService.Delete(ctx, gatewayApp.GatewayId); deleteErr != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error while cleaning up errored Livy GatewayApplication '%s': %w", gatewayApp.GatewayId, err))
		} else {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error inserting Livy GatewayApplication '%s' into database: %w", gatewayApp.GatewayId, err))
		}
	}

	return gatewayApp.ToLivyBatch(int32(livyApp.BatchID)), nil

}

func (l *livyService) Delete(ctx context.Context, batchId int) error {

	livyApp, err := l.database.GetByBatchId(ctx, batchId)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error getting GatewayApplication from Livy BatchId: %w", err))
	}

	if err := l.appService.Delete(ctx, livyApp.GatewayID); err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error deleting Livy GatewayApplication: %w", err))
	}

	return nil
}

func (l *livyService) Logs(ctx context.Context, batchId int, size int) ([]string, error) {
	livyApp, err := l.database.GetByBatchId(ctx, batchId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting GatewayApplication from Livy BatchId: %w", err))
	}

	logs, err := l.appService.Logs(ctx, livyApp.GatewayID, size)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting logs for Livy GatewayApplication: %w", err))
	}

	logSplice := strings.Split(*logs, "\n")

	return logSplice, nil

}
