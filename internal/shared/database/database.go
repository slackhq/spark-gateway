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
	"net/url"

	"k8s.io/klog/v2"

	"time"

	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/util"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"

	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

// statementTimeout bounds how long any single query may run on the server,
// protecting pooled connections from being held indefinitely by a wedged query.
const statementTimeout = "30s"

//go:generate moq -rm -out mocksparkapplicationdatabase.go . SparkApplicationDatabase

type SparkApplicationDatabase interface {
	GetById(ctx context.Context, gatewayIdUid uuid.UUID) (*SparkApplication, error)
	UpdateSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, updateSparkApp v1beta2.SparkApplication) error
	InsertSparkApplication(ctx context.Context, gatewayIdUid uuid.UUID, creationTime time.Time, userSubmittedSparkApp *v1beta2.SparkApplication, clusterName string) error
}

//go:generate moq -rm -out mocklivyapplicationdatabase.go . LivyApplicationDatabase


type LivyApplicationDatabase interface {
	GetByBatchId(ctx context.Context, batchId int) (LivyApplication, error)
	ListFrom(ctx context.Context, fromId int, size int) ([]LivyApplication, error)
	InsertLivyApplication(ctx context.Context, gatewayId string) (LivyApplication, error)
}

type Database struct {
	connectionPool *pgxpool.Pool
}

func GetConnectionPool(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database connection string: %w", err)
	}

	// Tune the pool so it does not grow unbounded and recycles connections,
	// which keeps connections fresh against Postgres/PgBouncer.
	poolConfig.MaxConns = 20
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute
	poolConfig.HealthCheckPeriod = time.Minute

	dbpool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	return dbpool, nil
}

func GetConnectionString(databaseConfig config.Database) string {

	// postgres://{user}:{password}@{hostname}:{port}/{database-name}?statement_timeout=...
	klog.Infof("Database Info: %s:%s/%s\n",
		databaseConfig.Hostname,
		databaseConfig.Port,
		databaseConfig.DatabaseName,
	)

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(databaseConfig.Username, databaseConfig.Password),
		Host:     fmt.Sprintf("%s:%s", databaseConfig.Hostname, databaseConfig.Port),
		Path:     databaseConfig.DatabaseName,
		RawQuery: url.Values{"statement_timeout": {statementTimeout}}.Encode(),
	}
	return dsn.String()
}

func NewDatabase(ctx context.Context, dbConfig config.Database) (*Database, error) {
	connectionString := GetConnectionString(dbConfig)
	connectionPool, err := GetConnectionPool(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("could not create DatabaseRepository connection: %w", err)
	}

	return &Database{
		connectionPool: connectionPool,
	}, nil
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

	queryParams := InsertSparkApplicationParams{
		Uid:          gatewayIdUid,
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

// GetByBatchId returns the GatewayId of the corresponding SparkApplication the batchId maps too
func (db *Database) GetByBatchId(ctx context.Context, batchId int) (LivyApplication, error) {
	queries := New(db.connectionPool)

	gatewayId, err := queries.GetByBatchId(ctx, int64(batchId))
	if err != nil {
		return LivyApplication{}, gatewayerrors.NewFrom(fmt.Errorf("error getting SparkApplication with Livy BatchId '%d' from database: %w", batchId, err))
	}

	return gatewayId, nil
}

// ListFrom returns a list of GatewayIds of the corresponding SparkApplicaitions starting at "from" batchId
// and including up to "size" applications from that id
func (db *Database) ListFrom(ctx context.Context, from int, size int) ([]LivyApplication, error) {
	queries := New(db.connectionPool)

	livyApps, err := queries.ListFrom(ctx, ListFromParams{
		BatchID: int64(from),
		Size:    int32(size),
	})

	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error listing SparkApplications: %w", err))
	}

	return livyApps, nil
}

func (db *Database) InsertLivyApplication(ctx context.Context, gatewayId string) (LivyApplication, error) {
	queries := New(db.connectionPool)

	livyBatch, err := queries.InsertLivyApplication(ctx, gatewayId)
	if err != nil {
		return LivyApplication{}, gatewayerrors.NewFrom(fmt.Errorf("erroring inserting Livy SparkApplication '%s' into database: %w", gatewayId, err))
	}

	return livyBatch, nil
}
