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

package service

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/domain"
	clusterPkg "github.com/slackhq/spark-gateway/internal/gateway/repository"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/util"

	"github.com/slackhq/spark-gateway/internal/gateway/clusterrouter"
	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

//go:generate moq -rm  -out mocksparkapplicationrepository.go . GatewayApplicationRepository

type GatewayApplicationRepository interface {
	Get(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) (*domain.GatewayApplication, error)
	List(ctx context.Context, cluster domain.KubeCluster, namespace string) ([]*domain.GatewayApplicationMeta, error)
	Status(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) (*domain.GatewayApplicationStatus, error)
	Logs(ctx context.Context, cluster domain.KubeCluster, namespace string, name string, tailLines int) (*string, error)
	Create(ctx context.Context, cluster domain.KubeCluster, gatewayApp *domain.GatewayApplication) (*domain.GatewayApplication, error)
	Delete(ctx context.Context, cluster domain.KubeCluster, namespace string, name string) error
}

//go:generate moq -rm  -out mockgatewayapplicationservice.go . GatewayApplicationService

type GatewayApplicationService interface {
	Get(ctx context.Context, gatewayId string) (*domain.GatewayApplication, error)
	List(ctx context.Context, cluster string, namespace string) ([]*domain.GatewayApplicationMeta, error)
	Create(ctx context.Context, gatewayApp *domain.GatewayApplication) (*domain.GatewayApplication, error)
	Status(ctx context.Context, gatewayId string) (*domain.GatewayApplicationStatus, error)
	Logs(ctx context.Context, gatewayId string, tailLines int) (*string, error)
	Delete(ctx context.Context, gatewayId string) error
}

type service struct {
	gatewayAppRepo        GatewayApplicationRepository
	clusterRepository     clusterPkg.ClusterRepository
	clusterRouter         clusterrouter.ClusterRouter
	fallbackClusterRouter clusterrouter.ClusterRouter
	config                config.GatewayConfig
	selectorKey           string
	selectorValue         string
}

func NewApplicationService(
	gatewayAppRepo GatewayApplicationRepository,
	clusterRepository clusterPkg.ClusterRepository,
	clusterRouter clusterrouter.ClusterRouter,
	fallbackClusterRouter clusterrouter.ClusterRouter,
	config config.GatewayConfig,
	selectorKey string,
	selectorValue string,
) GatewayApplicationService {
	return &service{
		gatewayAppRepo:        gatewayAppRepo,
		clusterRepository:     clusterRepository,
		clusterRouter:         clusterRouter,
		fallbackClusterRouter: fallbackClusterRouter,
		config:                config,
		selectorKey:           selectorKey,
		selectorValue:         selectorValue,
	}
}

func (s *service) GetClusterNamespaceFromGatewayId(gatewayId string) (*domain.KubeCluster, string, error) {
	clusterId := strings.Split(gatewayId, "-")[0]
	kubeCluster, err := s.clusterRepository.GetById(clusterId)

	if err != nil {
		return nil, "", gatewayerrors.NewInternal(fmt.Errorf("error getting cluster parsed from gatewayId: %w", err))
	}

	namespaceId := strings.Split(gatewayId, "-")[1]
	namespace, err := kubeCluster.GetNamespaceById(namespaceId)

	if err != nil {
		return nil, "", gatewayerrors.NewInternal(fmt.Errorf("error getting namespace from cluster '%s': %w", kubeCluster.Name, err))
	}
	return kubeCluster, namespace.Name, nil
}

func (s *service) Get(ctx context.Context, gatewayId string) (*domain.GatewayApplication, error) {

	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	gatewayApp, err := s.gatewayAppRepo.Get(ctx, *cluster, namespace, gatewayId)

	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting GatewayApplication '%s': %w", gatewayId, err))
	}

	return gatewayApp, nil
}

// List retrieves `num` number of GatewayApplications from specified namespace `namespace` in cluster `cluster`
func (s *service) List(ctx context.Context, cluster string, namespace string) ([]*domain.GatewayApplicationMeta, error) {

	kubeCluster, err := s.clusterRepository.GetByName(cluster)

	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting cluster: %w", err))
	}

	namespaces := []string{}
	// Get all apps in cluster if namespace is blank
	if namespace != "" {
		if _, err := kubeCluster.GetNamespaceByName(namespace); err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting namespace: %w", err))
		}
		namespaces = append(namespaces, namespace)
	} else {
		for _, kubeNamespace := range kubeCluster.Namespaces {
			namespaces = append(namespaces, kubeNamespace.Name)
		}
	}

	var gatewayMetaList []*domain.GatewayApplicationMeta
	for _, ns := range namespaces {
		nsGatewayMeta, err := s.gatewayAppRepo.List(ctx, *kubeCluster, ns)
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting applications: %w", err))
		}

		for _, gatewayMeta := range nsGatewayMeta {
			gatewayMetaList = append(gatewayMetaList, gatewayMeta)
		}

	}

	return gatewayMetaList, nil

}

func (s *service) Create(ctx context.Context, gatewayApp *domain.GatewayApplication) (*domain.GatewayApplication, error) {

	cluster, err := s.clusterRouter.GetCluster(ctx, gatewayApp.Namespace)
	if cluster == nil || err != nil {
		klog.Warningf("error getting cluster for gatewayApp '%s': %v", gatewayApp.Name, err)
		klog.Warning("Trying fallback cluster router")
		// Try fallback cluster router
		cluster, err = s.fallbackClusterRouter.GetCluster(ctx, gatewayApp.Namespace)
		if cluster == nil || err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting routing cluster: %w", err))
		}
	}

	// Generate name from clusterId and UUID and set
	gatewayId, err := domain.NewId(*cluster, gatewayApp.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error generating GatewayId for GatewayApplication: %w", err)
	}

	gatewayApp = s.SetGatewayApplicationOverrides(gatewayApp, gatewayId)

	// Create SparkApp
	createdApp, err := s.gatewayAppRepo.Create(ctx, *cluster, gatewayApp)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating GatewayApplication '%s/%s': %w", gatewayApp.Namespace, gatewayApp.Name, err))
	}

	return createdApp, nil
}

func (s *service) Status(ctx context.Context, gatewayId string) (*domain.GatewayApplicationStatus, error) {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	gatewayApp, err := s.gatewayAppRepo.Get(ctx, *cluster, namespace, gatewayId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting status for GatewayApplication '%s': %w", gatewayId, err))
	}

	return &gatewayApp.Status, nil
}

func (s *service) Logs(ctx context.Context, gatewayId string, tailLines int) (*string, error) {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	logString, err := s.gatewayAppRepo.Logs(ctx, *cluster, namespace, gatewayId, tailLines)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting Logs for GatewayApplication '%s': %w", gatewayId, err))
	}

	return logString, nil
}

func (s *service) Delete(ctx context.Context, gatewayId string) error {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return err
	}

	if err := s.gatewayAppRepo.Delete(ctx, *cluster, namespace, gatewayId); err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error deleting GatewayApplication '%s': %w", gatewayId, err))
	}

	return nil
}

func GetRenderedURLs(templates domain.StatusUrlTemplates, gatewayApp *domain.GatewayApplication) domain.StatusUrlTemplates {
	// Render URLs
	sparkUI, err := util.RenderTemplate(templates.SparkUI, gatewayApp)
	if err != nil {
		klog.Errorf("unable to render SparkUI template: %v", err)
		sparkUI = new(string)
	}

	logsUI, err := util.RenderTemplate(templates.LogsUI, gatewayApp)
	if err != nil {
		klog.Errorf("unable to render LogsUI template: %v", err)
		logsUI = new(string)
	}

	sparkHistoryUI, err := util.RenderTemplate(templates.SparkHistoryUI, gatewayApp)
	if err != nil {
		klog.Errorf("unable to render SparkHistoryUI template: %v", err)
		sparkHistoryUI = new(string)
	}

	return domain.StatusUrlTemplates{
		SparkUI:        *sparkUI,
		LogsUI:         *logsUI,
		SparkHistoryUI: *sparkHistoryUI,
	}
}

// SetGatewayApplicationOverrides sets values of the GatewayApplication that are determined at the service layer
func (s *service) SetGatewayApplicationOverrides(gatewayApp *domain.GatewayApplication, gatewayId string) *domain.GatewayApplication {

	// Set Name to GatewayId
	gatewayApp.Name = gatewayId

	// Set selector labels
	if s.selectorKey != "" && s.selectorValue != "" {
		gatewayApp.Labels[s.selectorKey] = s.selectorValue
	}

	return gatewayApp
}
