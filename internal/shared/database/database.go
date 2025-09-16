// Copyright (c) 2025, Salesforce, Inc.
// SPDX-License-Identifier: Apache-2
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"k8s.io/klog/v2"

	"time"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/util"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"

	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

//go:generate moq -rm -out mockdatabaserepository.go . DatabaseRepository

type SparkApplicationDatabaseRepository interface {
	GetById(ctx context.Context, gatewayIdUid uuid.UUID) (*SparkApplication, error)
	UpdateSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, updateSparkApp v1beta2.SparkApplication) error
	InsertSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, creationTime time.Time, userSubmittedSparkApp *v1beta2.SparkApplication, clusterName string) error
}

type LivyApplicationDatabaseRepository interface {
	GetByBatchId(ctx context.Context, batchId int) (*SparkApplication, error)
	ListFrom(ctx context.Context, fromId int, size int) ([]*SparkApplication, error)
}

type Database struct {
	connectionPool *pgxpool.Pool
}

func GetConnectionPool(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("Unable to create connection pool: %v\n", err)
	}

	return dbpool, nil
}

func GetConnectionString(databaseConfig config.Database) string {

	// postgres://{user}:{password}@{hostname}:{port}/{database-name}
	klog.Infof("Database Info: %s:%s/%s\n",
		databaseConfig.Hostname,
		databaseConfig.Port,
		databaseConfig.DatabaseName,
	)
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		databaseConfig.Username,
		databaseConfig.Password,
		databaseConfig.Hostname,
		databaseConfig.Port,
		databaseConfig.DatabaseName,
	)
}

func NewDatabase(ctx context.Context, dbConfig config.Database) *Database {
	connectionString := GetConnectionString(dbConfig)
	connectionPool, err := GetConnectionPool(ctx, connectionString)
	if err != nil {
		klog.Fatal(fmt.Errorf("could not create DatabaseRepository connection: %w", err))
	}

	return &Database{
		connectionPool: connectionPool,
	}
}

func (db *Database) GetById(ctx context.Context, gatewayIdUid uuid.UUID) (*SparkApplication, error) {
	queries := New(db.connectionPool)
	sparkApp, err := queries.GetById(ctx, gatewayIdUid)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting SparkApplication '%s' from database: %w", gatewayIdUid, err))
	}

	return &sparkApp, nil

}

func (db *Database) UpdateSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, updateSparkApp v1beta2.SparkApplication) error {
	state := string(updateSparkApp.Status.AppState.State)

	// Marshal the SparkApplication for DB
	jsonSparkAppStatus, err := json.Marshal(updateSparkApp.Status)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error marshaling SparkApplication.Status '%s/%s': %w", updateSparkApp.Namespace, updateSparkApp.Name, err))
	}

	jsonSparkApp, err := json.Marshal(updateSparkApp)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error marshaling SparkApplication '%s/%s': %w", updateSparkApp.Namespace, updateSparkApp.Name, err))
	}

	var terminationTime *time.Time = nil
	if updateSparkApp.Status.TerminationTime.Time != (time.Time{}) {
		terminationTime = &updateSparkApp.Status.TerminationTime.Time
	}

	queryParams := UpdateSparkApplicationParams{
		Uid:             gatewayIdUid,
		TerminationTime: terminationTime,
		Updated:         jsonSparkApp,
		State:           &state,
		Status:          jsonSparkAppStatus,
	}

	// sqlc generated methods
	queries := New(db.connectionPool)
	updatedSparkAppRow, err := queries.UpdateSparkApplication(ctx, queryParams)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error updating SparkApplication '%s/%s' to database: %w", updateSparkApp.Namespace, updateSparkApp.Name, err))
	}

	SparkAppAuditLog(gatewayIdUid, updatedSparkAppRow)

	return nil
}

func (db *Database) InsertSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, creationTime time.Time, userSubmittedSparkApp *v1beta2.SparkApplication, clusterName string) error {

	// Marshal the user submitted SparkApplication for audit log
	// Will be written to database if submission is successful
	jsonAppSubmitted, err := json.Marshal(userSubmittedSparkApp)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error marshaling SparkApplication '%s/%s': %w", userSubmittedSparkApp.Namespace, userSubmittedSparkApp.Name, err))
	}

	// If livy is enabled, this will be set otherwise keep null
	var batchId *int64
	if batchIdLabel, ok := userSubmittedSparkApp.Labels[domain.LIVY_BATCH_ID_LABEL]; ok {
		batchIdInt, err := strconv.Atoi(batchIdLabel)
		if err != nil {
			return gatewayerrors.NewFrom(fmt.Errorf("error converting batch id to insert to database: %w", err))
		}

		batch64 := int64(batchIdInt)
		batchId = &batch64
	}

	queryParams := InsertSparkApplicationParams{
		Uid:          gatewayIdUid,
		BatchID:      batchId,
		Name:         &userSubmittedSparkApp.ObjectMeta.Name,
		CreationTime: &creationTime,
		Username:     userSubmittedSparkApp.Spec.ProxyUser,
		Namespace:    &userSubmittedSparkApp.ObjectMeta.Namespace,
		Cluster:      &clusterName,
		Submitted:    jsonAppSubmitted,
	}

	// Write SparkApp to database
	// sqlc generated methods
	queries := New(db.connectionPool)
	insertedSparkApp, err := queries.InsertSparkApplication(ctx, queryParams)
	if err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error inserting SparkApplication '%s/%s' to database: %w", userSubmittedSparkApp.Namespace, userSubmittedSparkApp.Name, err))
	}

	SparkAppAuditLog(gatewayIdUid, insertedSparkApp)

	return nil
}

func SparkAppAuditLog(gatewayIdUid uuid.UUID, sparkApp SparkApplication) {
	klog.Infof("SparkApplication Updated in DB: gatewayIdUid: %s, name: %s, namespace: %s, cluster: %s, creation_time: %s, username: %s",
		gatewayIdUid,
		util.SafeString(sparkApp.Name),
		util.SafeString(sparkApp.Namespace),
		util.SafeString(sparkApp.Cluster),
		util.SafeTime(sparkApp.CreationTime),
		util.SafeString(sparkApp.Username),
	)
}

// Livy
func (db *Database) GetByBatchId(ctx context.Context, batchId int) (*SparkApplication, error) {
	queries := New(db.connectionPool)

	dbId := int64(batchId)
	sparkApp, err := queries.GetByBatchId(ctx, &dbId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting SparkApplication with Livy BatchId '%d' from database: %w", batchId, err))
	}

	return &sparkApp, nil
}

func (db *Database) ListFrom(ctx context.Context, from int, size int) ([]*SparkApplication, error) {
	queries := New(db.connectionPool)

	from64 := int64(from)
	sparkApps, err := queries.ListFrom(ctx, ListFromParams{
		Fromid: &from64,
		Size:   int32(size),
	})

	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error listing SparkApplications: %w", err))
	}

	var retApps []*SparkApplication
	for _, app := range sparkApps {
		retApps = append(retApps, &app)
	}

	return retApps, nil
}
