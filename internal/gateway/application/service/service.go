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
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/gateway/application/handler"
	clusterPkg "github.com/slackhq/spark-gateway/internal/gateway/cluster"
	"github.com/slackhq/spark-gateway/pkg/config"
	"github.com/slackhq/spark-gateway/pkg/util"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"

	"github.com/slackhq/spark-gateway/internal/gateway/router"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/model"
)

//go:generate moq -rm  -out mocksparkapplicationrepository.go . SparkApplicationRepository

type SparkApplicationRepository interface {
	Get(ctx context.Context, cluster string, namespace string, name string) (*v1beta2.SparkApplication, error)
	List(ctx context.Context, cluster string, namespace string) ([]*model.SparkManagerApplicationMeta, error)
	Status(ctx context.Context, cluster string, namespace string, name string) (*v1beta2.SparkApplicationStatus, error)
	Logs(ctx context.Context, cluster string, namespace string, name string, tailLines int) (*string, error)
	Create(ctx context.Context, cluster string, application *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error)
	Delete(ctx context.Context, cluster string, namespace string, name string) error
}

type service struct {
	sparkAppRepo          SparkApplicationRepository
	clusterRepository     clusterPkg.ClusterRepository
	clusterRouter         router.ClusterRouter
	fallbackClusterRouter router.ClusterRouter
	config                config.GatewayConfig
	selectorKey           string
	selectorValue         string
	gatewayIdGenerator    model.GatewayIdGenerator
}

func NewApplicationService(
	sparkAppRepo SparkApplicationRepository,
	clusterRepository clusterPkg.ClusterRepository,
	clusterRouter router.ClusterRouter,
	fallbackClusterRouter router.ClusterRouter,
	config config.GatewayConfig,
	selectorKey string,
	selectorValue string,
	gatewayIdGenerator model.GatewayIdGenerator,
) handler.GatewayApplicationService {
	return &service{
		sparkAppRepo:          sparkAppRepo,
		clusterRepository:     clusterRepository,
		clusterRouter:         clusterRouter,
		fallbackClusterRouter: fallbackClusterRouter,
		config:                config,
		selectorKey:           selectorKey,
		selectorValue:         selectorValue,
		gatewayIdGenerator:    gatewayIdGenerator,
	}
}

func (s *service) GetClusterNamespaceFromGatewayId(gatewayId string) (*model.KubeCluster, string, error) {
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

func (s *service) Get(ctx context.Context, gatewayId string) (*model.GatewayApplication, error) {

	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	sparkApp, err := s.sparkAppRepo.Get(ctx, cluster.Name, namespace, gatewayId)

	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting SparkApplication '%s': %w", gatewayId, err))
	}

	user, ok := sparkApp.Labels[model.GATEWAY_USER_LABEL]
	if !ok {
		return nil, gatewayerrors.NewFrom(errors.New("no gateway user associated with this application, possibly not created through spark-gateway?"))
	}

	gatewayApp := &model.GatewayApplication{
		SparkApplication: sparkApp,
		GatewayId:        sparkApp.Name,
		Cluster:          cluster.Name,
		User:             user,
		SparkLogURLs:     GetRenderedURLs(s.config.StatusUrlTemplates, sparkApp),
	}

	return gatewayApp, nil
}

// List retrieves a list of GatewayApplicationMeta for all SparkApplications in the specified namespace and cluster.
func (s *service) List(ctx context.Context, cluster string, namespace string) ([]*model.GatewayApplicationMeta, error) {

	var kubeClusters []model.KubeCluster
	var err error

	if cluster == "all" {
		kubeClusters, err = s.clusterRepository.GetAll()
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting all clusters: %w", err))
		}
	} else {
		kubeCluster, err := s.clusterRepository.GetByName(cluster)
		if err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting cluster: %w", err))
		}
		kubeClusters = []model.KubeCluster{*kubeCluster}
	}

	namespacesMap := make(map[string][]string)

	for _, cluster := range kubeClusters {
		namespaceList := []string{}
		if namespace == "all" {
			for _, ns := range cluster.Namespaces {
				namespaceList = append(namespaceList, ns.Name)
			}
		} else {
			if _, err := cluster.GetNamespaceByName(namespace); err != nil {
				return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting namespace: %w", err))
			}
			namespaceList = append(namespaceList, namespace)
		}
		namespacesMap[cluster.Name] = namespaceList
	}

	fetchSparkAppMeta := func(ctx context.Context, cluster string, namespaceList []string, outputChan chan []*model.GatewayApplicationMeta, outputErrChan chan error) {
		appMetaList := []*model.GatewayApplicationMeta{}

		for _, ns := range namespaceList {
			// Check if context is done
			select {
			case <-ctx.Done():
				return
			default:
			}

			nsAppMetas, err := s.sparkAppRepo.List(ctx, cluster, ns)
			if err != nil {
				select {
				case outputErrChan <- fmt.Errorf("error getting applications for %s cluster and %s namespace: %w", cluster, ns, err):
				case <-ctx.Done():
				}
				return
			}

			for _, appMeta := range nsAppMetas {
				appMetaList = append(appMetaList, model.NewGatewayApplicationMeta(appMeta, cluster))
			}
		}

		select {
		case outputChan <- appMetaList:
		case <-ctx.Done():
		}
	}

	receiveChan := make(chan []*model.GatewayApplicationMeta)
	receiveErrChan := make(chan error)

	// close channels when ctx is cancelled
	go func() {
		// context comes from gin request and should be cancelled after the response is sent
		<-ctx.Done()
		close(receiveChan)
		close(receiveErrChan)
	}()

	// Create a routine per cluster
	for cluster, namespaceList := range namespacesMap {
		go fetchSparkAppMeta(ctx, cluster, namespaceList, receiveChan, receiveErrChan)
	}

	var totalAppMetaList []*model.GatewayApplicationMeta

	timeoutSeconds := 60 * time.Second
	listTimeout := time.After(timeoutSeconds)

	// Fetch responses
	for range len(namespacesMap) {
		select {
		case appMetaList := <-receiveChan:
			totalAppMetaList = append(totalAppMetaList, appMetaList...)
		case err := <-receiveErrChan:
			return nil, gatewayerrors.NewInternal(fmt.Errorf("List call to SparkManager returned error: %w", err))
		case <-listTimeout:
			return nil, gatewayerrors.NewInternal(fmt.Errorf("Timeout reached for list calls to SparkManager. Timeout: %v", timeoutSeconds))
		case <-ctx.Done():
			return nil, gatewayerrors.NewInternal(errors.New("Context is cancelled"))
		}
	}

	return totalAppMetaList, nil

}

func (s *service) Create(ctx context.Context, application *v1beta2.SparkApplication, user string) (*model.GatewayApplication, error) {

	errors := s.SparkApplicationValidator(application)
	if len(errors) > 0 {
		return nil, gatewayerrors.NewBadRequest(fmt.Errorf("submitted SparkApplication is invalid: %v", errors))
	}

	application = SparkApplicationDefaulter(application)

	cluster, err := s.clusterRouter.GetCluster(ctx, application.ObjectMeta.Namespace)
	if cluster == nil || err != nil {
		klog.Warningf("error getting cluster for application '%s': %v", application.ObjectMeta.Name, err)
		klog.Warning("Trying fallback cluster router")
		// Try fallback cluster router
		cluster, err = s.fallbackClusterRouter.GetCluster(ctx, application.ObjectMeta.Namespace)
		if cluster == nil || err != nil {
			return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting routing cluster: %w", err))
		}
	}

	// Generate name from clusterId and UUID and set
	appName, err := s.gatewayIdGenerator.NewId(*cluster, application.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error generating GatewayId for SparkApplication: %w", err)
	}

	application = s.SparkApplicationOverrides(application, user, appName)

	// Create SparkApp
	sparkApp, err := s.sparkAppRepo.Create(ctx, cluster.Name, application)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error creating SparkApplication '%s/%s': %w", application.Namespace, application.Name, err))
	}

	gatewayApp := &model.GatewayApplication{
		SparkApplication: sparkApp,
		GatewayId:        sparkApp.Name,
		Cluster:          cluster.Name,
		User:             *sparkApp.Spec.ProxyUser,
		SparkLogURLs:     GetRenderedURLs(s.config.StatusUrlTemplates, sparkApp),
	}

	return gatewayApp, nil
}

func (s *service) Status(ctx context.Context, gatewayId string) (*v1beta2.SparkApplicationStatus, error) {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	sparkApp, err := s.sparkAppRepo.Get(ctx, cluster.Name, namespace, gatewayId)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting status for SparkApplication '%s': %w", gatewayId, err))
	}

	return &sparkApp.Status, nil
}

func (s *service) Logs(ctx context.Context, gatewayId string, tailLines int) (*string, error) {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return nil, err
	}

	logString, err := s.sparkAppRepo.Logs(ctx, cluster.Name, namespace, gatewayId, tailLines)
	if err != nil {
		return nil, gatewayerrors.NewFrom(fmt.Errorf("error getting Logs for SparkApplication '%s': %w", gatewayId, err))
	}

	return logString, nil
}

func (s *service) Delete(ctx context.Context, gatewayId string) error {
	cluster, namespace, err := s.GetClusterNamespaceFromGatewayId(gatewayId)
	if err != nil {
		return err
	}

	if err := s.sparkAppRepo.Delete(ctx, cluster.Name, namespace, gatewayId); err != nil {
		return gatewayerrors.NewFrom(fmt.Errorf("error deleting SparkApplication '%s': %w", gatewayId, err))
	}

	return nil
}

func GetRenderedURLs(templates model.StatusUrlTemplates, sparkApp *v1beta2.SparkApplication) model.SparkLogURLs {
	// Render URLs
	sparkUI, err := util.RenderTemplate(templates.SparkUI, sparkApp)
	if err != nil {
		klog.Errorf("unable to render SparkUI template: %v", err)
		sparkUI = new(string)
	}

	logsUI, err := util.RenderTemplate(templates.LogsUI, sparkApp)
	if err != nil {
		klog.Errorf("unable to render LogsUI template: %v", err)
		logsUI = new(string)
	}

	sparkHistoryUI, err := util.RenderTemplate(templates.SparkHistoryUI, sparkApp)
	if err != nil {
		klog.Errorf("unable to render SparkHistoryUI template: %v", err)
		sparkHistoryUI = new(string)
	}

	return model.SparkLogURLs{
		SparkUI:        *sparkUI,
		LogsUI:         *logsUI,
		SparkHistoryUI: *sparkHistoryUI,
	}
}

func (s *service) SparkApplicationValidator(application *v1beta2.SparkApplication) []string {
	var errors []string
	if application == nil {
		errors = append(errors, "application should never be nil")
		return errors
	}

	if application.ObjectMeta.Namespace == "" {
		errors = append(errors, "namespace should not be empty")
	}

	return errors
}

func SparkApplicationDefaulter(application *v1beta2.SparkApplication) *v1beta2.SparkApplication {
	klog.Info("setting defaults for SparkApplication")
	// If SparkApp has a name, set it as an annotation
	/// Set annotations first
	if application.Annotations == nil {
		application.Annotations = map[string]string{}
	}

	if application.Labels == nil {
		application.Labels = map[string]string{}
	}

	return application
}

func (s *service) SparkApplicationOverrides(application *v1beta2.SparkApplication, user string, appName string) *v1beta2.SparkApplication {
	if application.Name != "" {
		application.Annotations["applicationName"] = application.Name
	}

	application.Name = appName

	// Set default gateway labels
	gatewayLabels := map[string]string{
		model.GATEWAY_USER_LABEL: user,
	}

	// Set selector label
	if s.selectorKey != "" && s.selectorValue != "" {
		gatewayLabels = util.MergeMaps(gatewayLabels, map[string]string{
			s.selectorKey: s.selectorValue,
		})
	}

	// Merge labels if they exist in the submitted SparkApp
	application.Labels = util.MergeMaps(application.Labels, gatewayLabels)

	// set user
	application.Spec.ProxyUser = &user

	return application
}
