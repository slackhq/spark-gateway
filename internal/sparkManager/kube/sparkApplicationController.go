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

package kube

import (
	"context"
	"fmt"
	"time"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkClientSet "github.com/kubeflow/spark-operator/v2/pkg/client/clientset/versioned"
	sparkOpInformer "github.com/kubeflow/spark-operator/v2/pkg/client/informers/externalversions"
	v1beta2Lister "github.com/kubeflow/spark-operator/v2/pkg/client/listers/api/v1beta2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	dbRepository "github.com/slackhq/spark-gateway/pkg/database/repository"
	"github.com/slackhq/spark-gateway/pkg/gatewayerrors"
	"github.com/slackhq/spark-gateway/pkg/model"
)

// SparkController is a type of Kubernetes controller. The difference between SparkController and a typical
// Kubernetes controller is that SparkController is only being used for it Informer components.
// SparkController here should not reconcile or modify Spark Application resources since that's the responsibility of
// the Spark Operator, and we want to keep SparkController compatible with the Spark Operator, avoiding any race
// conditions with the controller in the Spark Operator.
// Reference: https://github.com/kubernetes/sample-controller/blob/master/controller.go

type SparkController struct {
	SparkInformer cache.SharedIndexInformer
	SparkLister   v1beta2Lister.SparkApplicationLister
	ctx           context.Context
	clusterName   string
	database      dbRepository.DatabaseRepository
}

func NewSparkController(
	ctx context.Context,
	kubeConfig *rest.Config,
	selectorKey string,
	selectorValue string,
	clusterName string,
	database dbRepository.DatabaseRepository,
) (*SparkController, error) {

	sparkClient, err := sparkClientSet.NewForConfig(kubeConfig)
	if err != nil {
		return nil, gatewayerrors.MapK8sErrorToGatewayError(fmt.Errorf("error creating spark client: %w", err))
	}

	// Filter SparkApps by selector label if set
	sharedInformerOption := sparkOpInformer.WithTweakListOptions(func(options *v1.ListOptions) {})
	if selectorKey != "" && selectorValue != "" {
		labelSelector := fmt.Sprintf("%s=%s", selectorKey, selectorValue)
		klog.Infof("Spark Gateway Indexer monitoring LabelSelector: %s", labelSelector)
		sharedInformerOption = sparkOpInformer.WithTweakListOptions(func(options *v1.ListOptions) {
			options.LabelSelector = labelSelector
		})
	}

	// Create an instance of SharedInformerFactory with additional options
	// Refresh every 30 seconds
	informerFactory := sparkOpInformer.NewSharedInformerFactoryWithOptions(
		sparkClient,
		30*time.Second,
		sharedInformerOption,
	)

	controller := &SparkController{
		SparkInformer: informerFactory.Sparkoperator().V1beta2().SparkApplications().Informer(),
		SparkLister:   informerFactory.Sparkoperator().V1beta2().SparkApplications().Lister(),
		ctx:           ctx,
		clusterName:   clusterName,
		database:      database,
	}

	_, err = controller.SparkInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.onAdd,
			UpdateFunc: controller.onUpdate,
			DeleteFunc: controller.onDelete,
		})

	if err != nil {
		return nil, err
	}

	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	informerFactory.Start(ctx.Done())

	controller.Run()

	return controller, nil

}

// Do not modify SparkApplication resources in these event handlers, it could lead to race conditions between event
// handlers here and those in the Spark Operator. Any logic to modify SparkApplications should be implemented in the
// kubeflow/spark-operator project.
func (c *SparkController) onAdd(obj interface{}) {
	logger := klog.FromContext(context.Background())
	if sparkApp, ok := obj.(*v1beta2.SparkApplication); ok {
		logger.Info("SparkApp added",
			"namespace", sparkApp.Namespace,
			"name", sparkApp.Name)
	}
}

func (c *SparkController) onUpdate(oldObj interface{}, newObj interface{}) {
	logger := klog.FromContext(context.Background())

	oldSparkApp, oldOK := oldObj.(*v1beta2.SparkApplication)
	newSparkApp, newOK := newObj.(*v1beta2.SparkApplication)

	if !oldOK || !newOK {
		klog.Errorf("onUpdate: Unable to cast to v1beta2.SparkApplication: oldObj=%v, oldOK=%v", oldOK, newOK)
		return
	}

	gatewayIdUid, err := model.ParseGatewayIdUUID(newSparkApp.Name)
	if err != nil {
		klog.ErrorS(err, "Failed to parse the gateway UUID, skipping onUpdate", "gatewayId", newSparkApp.Name)
		return
	}
	changedFields := changedStatusFields(oldSparkApp, newSparkApp)
	if len(changedFields) > 0 {
		// Update SparkApp Row in DB when any subfields of Status are updated except for ExecutorState
		logger.Info(fmt.Sprintf("Status Fields changed for %s/%s: %v\n", newSparkApp.Namespace, newSparkApp.Name, changedFields))

		if c.database != nil {
			err := c.database.UpdateSparkApplication(c.ctx, *gatewayIdUid, *newSparkApp)
			if err != nil {
				logger.Error(err, "Failed to update db: %s", err)
			}
		}
	}

	logger.Info("SparkApp updated",
		"namespace", newSparkApp.Namespace,
		"name", newSparkApp.Name)

}

func (c *SparkController) onDelete(obj interface{}) {
	logger := klog.FromContext(context.Background())
	if sparkApp, ok := obj.(*v1beta2.SparkApplication); ok {
		logger.Info("SparkApp deleted",
			"namespace", sparkApp.Namespace,
			"name", sparkApp.Name)
	}
}

func (c *SparkController) Run() {
	logger := klog.FromContext(c.ctx)
	logger.Info("Starting Spark controller")

	logger.Info("Syncing Cache")
	if ok := cache.WaitForNamedCacheSync("SparkInformer", c.ctx.Done(), c.SparkInformer.HasSynced); !ok {
		logger.Error(fmt.Errorf("failed to wait for caches to sync"), "")
		return
	}

	return
}

// Check of changes to any subfields of Status except for ExecutorState. Ignore ExecutorState because it contains the
// list of executors that are updated frequently, and when the TerminationTime field is updated, the whole spec
// including the executorState field will be updated.
func changedStatusFields(oldSparkApp, newSparkApp *v1beta2.SparkApplication) []string {
	var changed []string

	if oldSparkApp.Status.AppState != newSparkApp.Status.AppState {
		changed = append(changed, "AppState")
	}
	if oldSparkApp.Status.DriverInfo != newSparkApp.Status.DriverInfo {
		changed = append(changed, "DriverInfo")
	}
	if oldSparkApp.Status.ExecutionAttempts != newSparkApp.Status.ExecutionAttempts {
		changed = append(changed, "ExecutionAttempts")
	}
	if !oldSparkApp.Status.LastSubmissionAttemptTime.Time.Equal(newSparkApp.Status.LastSubmissionAttemptTime.Time) {
		changed = append(changed, "LastSubmissionAttemptTime")
	}
	if oldSparkApp.Status.SparkApplicationID != newSparkApp.Status.SparkApplicationID {
		changed = append(changed, "SparkApplicationID")
	}
	if oldSparkApp.Status.SubmissionAttempts != newSparkApp.Status.SubmissionAttempts {
		changed = append(changed, "SubmissionAttempts")
	}
	if oldSparkApp.Status.SubmissionID != newSparkApp.Status.SubmissionID {
		changed = append(changed, "SubmissionID")
	}
	if !oldSparkApp.Status.TerminationTime.Time.Equal(newSparkApp.Status.TerminationTime.Time) {
		changed = append(changed, "TerminationTime")
	}

	return changed
}
