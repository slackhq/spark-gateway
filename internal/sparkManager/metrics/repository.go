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

package metrics

import (
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/sparkManager/kube"
)

type Repository struct {
	controller *kube.SparkController
}

func NewRepository(controller *kube.SparkController) *Repository {
	return &Repository{
		controller: controller,
	}
}

/*
GetFilteredSparkApplicationsByCluster returns a filtered list of SparkApplication that exist in the cluster.
*/
func (r *Repository) GetFilteredSparkApplicationsByCluster() []*v1beta2.SparkApplication {
	allLabels := labels.SelectorFromSet(map[string]string{})
	sparkApplicationList, err := r.controller.SparkLister.List(allLabels)
	if err != nil {
		klog.Error(err)
	}
	return r.FilterSparkApplicationList(sparkApplicationList)
}

/*
GetFilteredSparkApplicationsByNamespace returns a filtered list of SparkApplication that exist in the given namespace
in the cluster.
*/
func (r *Repository) GetFilteredSparkApplicationsByNamespace(namespace string) []*v1beta2.SparkApplication {
	allLabels := labels.SelectorFromSet(map[string]string{})
	sparkApplicationList, err := r.controller.SparkLister.SparkApplications(namespace).List(allLabels)
	if err != nil {
		klog.Error(err)
	}
	return r.FilterSparkApplicationList(sparkApplicationList)
}

/*
FilterSparkApplicationList returns the subset of SparkApplications which have states in below list:
- v1beta2.ApplicationStateNew
- v1beta2.ApplicationStateSubmitted
- v1beta2.ApplicationStateRunning
- v1beta2.ApplicationStatePendingRerun
- v1beta2.ApplicationStateInvalidating
- v1beta2.ApplicationStateSucceeding
- v1beta2.ApplicationStateFailing
- v1beta2.ApplicationStateUnknown
*/
func (r *Repository) FilterSparkApplicationList(sparkApplicationList []*v1beta2.SparkApplication) []*v1beta2.SparkApplication {
	var MonitoredSparkApplicationStatesMap = map[string]bool{
		string(v1beta2.ApplicationStateNew):          true,
		string(v1beta2.ApplicationStateSubmitted):    true,
		string(v1beta2.ApplicationStateRunning):      true,
		string(v1beta2.ApplicationStatePendingRerun): true,
		string(v1beta2.ApplicationStateInvalidating): true,
		string(v1beta2.ApplicationStateSucceeding):   true,
		string(v1beta2.ApplicationStateFailing):      true,
		string(v1beta2.ApplicationStateUnknown):      true,
	}
	var filteredSparkApplicationList []*v1beta2.SparkApplication
	for _, sparkApplication := range sparkApplicationList {
		if MonitoredSparkApplicationStatesMap[string(sparkApplication.Status.AppState.State)] {
			filteredSparkApplicationList = append(filteredSparkApplicationList, sparkApplication)
		}
	}
	return filteredSparkApplicationList
}

/*
GetTotalCPUAllocation returns the combined CPU allocation for all SparkApplications in the sparkApplicationList arg.
*/
func (r *Repository) GetTotalCPUAllocation(sparkApplicationList []*v1beta2.SparkApplication) float64 {
	cpuAllocation := 0.0
	defaultMaxExecutorCount := float64(1000)
	for _, sparkApp := range sparkApplicationList {
		cpuAllocation += GetSparkAppCpuAllocation(sparkApp, defaultMaxExecutorCount)
	}
	return cpuAllocation
}

/*
GetSparkAppCpuAllocation returns the max CPU allocated to the driver and executors combined based on the SparkApplication
spec.

If dynamicAllocation is enabled and dynamicAllocation's MaxExecutors config is not set in SparkApplication Spec or
in sparkConf["spark.dynamicAllocation.maxExecutors"] then, Spark will default to infinite for maxExecutor value, in which
case GetSparkAppCpuAllocation will use the defaultMaxExecutorCount arg as the executor count.

If driver or executor core configs are not set or if there are parsing errors, default Driver and Executor CPU allocation
will be assumed to be 1.0 because the default value for spark.driver.cores and spark.executor.cores in some cluster
managers.

Assumptions:
- Spark Operator submission takes SparkApp spec config with precedence over sparkConf configs
- Only CPU requests are used, limits are ignored since it's not guaranteed capacity
- Driver CPU config precedence, high to low:
  - .Spec.Driver.CoreRequest (spark.kubernetes.driver.request.cores)
  - --conf spark.kubernetes.driver.request.cores
  - .Spec.Driver.Cores (spark.driver.cores)
  - --conf spark.driver.cores

- Executor CPU config precedence, high to low:
  - .Spec.Executor.CoreRequest (spark.kubernetes.executor.request.cores)
  - --conf spark.kubernetes.executor.request.cores
  - .Spec.Executor.Cores (spark.driver.cores)
  - --conf spark.driver.cores

- Executor Count config precedence, high to low:
  - dynamicAllocationEnabled = (.Spec.DynamicAllocation.Enabled || spark.dynamicAllocation.enabled)
  - if dynamicAllocationEnabled then .Spec.DynamicAllocation.MaxExecutors
  - if dynamicAllocationEnabled then --conf spark.dynamicAllocation.maxExecutors
  - if not dynamicAllocationEnabled then .Spec.Executor.Instances (spark.executor.instances)
  - if not dynamicAllocationEnabled then --conf spark.executor.instances
*/
func GetSparkAppCpuAllocation(sparkApp *v1beta2.SparkApplication, defaultMaxExecutorCount float64) float64 {
	defaultCpuCores := 1.0 // default value for spark.driver.cores and spark.executor.cores

	// Driver
	driverCores := defaultCpuCores
	k8sDriverCores := ParseK8sCoresValue(sparkApp.Spec.Driver.CoreRequest, sparkApp.Spec.SparkConf, "spark.kubernetes.driver.request.cores")
	if k8sDriverCores != 0 {
		driverCores = k8sDriverCores
	} else {
		sparkDriverCores := ParseCoresValue(sparkApp.Spec.Driver.Cores, sparkApp.Spec.SparkConf, "spark.driver.cores")
		if sparkDriverCores != 0 {
			driverCores = sparkDriverCores
		}
	}

	// Executor
	executorCores := defaultCpuCores
	k8sExecCores := ParseK8sCoresValue(sparkApp.Spec.Executor.CoreRequest, sparkApp.Spec.SparkConf, "spark.kubernetes.executor.request.cores")
	if k8sExecCores != 0 {
		executorCores = k8sExecCores
	} else {
		sparkExecCores := ParseCoresValue(sparkApp.Spec.Executor.Cores, sparkApp.Spec.SparkConf, "spark.executor.cores")
		if sparkExecCores != 0 {
			executorCores = sparkExecCores
		}
	}

	// Check if DynamicAllocation enabled
	dynamicAllocationEnabled := IsDynamicAllocationEnabled(sparkApp.Spec.DynamicAllocation, sparkApp.Spec.SparkConf)

	// Executor Count
	executorCount := defaultMaxExecutorCount
	if dynamicAllocationEnabled {
		dynamicAllocExecutorCount := ParseDynamicAllocExecutorCount(sparkApp.Spec.DynamicAllocation, sparkApp.Spec.SparkConf)
		if dynamicAllocExecutorCount != 0 {
			executorCount = dynamicAllocExecutorCount
		}
	} else {
		executorCount = ParseExecutorCount(sparkApp.Spec.Executor.Instances, sparkApp.Spec.SparkConf)
	}

	// Count total executor CPU allocation
	return driverCores + (executorCores * executorCount)
}

/*
ParseK8sCoresValue returns the CPU cores value when the units for config values are in Kubernetes CPU units. SparkApplication
spec value (specValue arg), if set, will take precedence over the SparkConf value (sparkConf[sparkConfKey]), if set.
ParseK8sCoresValue will return float64(0) if spec value and sparkConf values are not set or if there is a parsing error.
Kubernetes CPU resource units: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
*/
func ParseK8sCoresValue(specValue *string, sparkConf map[string]string, sparkConfKey string) float64 {
	cores := float64(0)
	var err error = nil
	if specValue != nil {
		cores, err = ParseKubeCPU(*specValue)
		if err != nil {
			klog.Error(err)
		}
	} else if val, ok := sparkConf[sparkConfKey]; ok {
		cores, err = ParseKubeCPU(val)
		if err != nil {
			klog.Error(err)
		}
	}
	return cores
}

/*
ParseCoresValue returns the CPU cores value. SparkApplication spec value (specValue arg), if set, will take precedence
over the SparkConf value (sparkConf[sparkConfKey]), if set. ParseCoresValue will return float64(0) if spec value and
sparkConf values are not set or if there is a parsing error.
*/
func ParseCoresValue(specValue *int32, sparkConf map[string]string, sparkConfKey string) float64 {
	cores := float64(0)
	var err error = nil
	if specValue != nil {
		cores = float64(*specValue)
	} else if val, ok := sparkConf[sparkConfKey]; ok {
		cores, err = strconv.ParseFloat(val, 64)
		if err != nil {
			klog.Error(err)
		}
	}
	return cores
}

/*
ParseDynamicAllocExecutorCount will return the number of max executors allocated based on DynamicAllocation maxExecutors
config. DynamicAllocation spec value (dynamicAllocationSpec arg), if set, will take precedence over the SparkConf value
(sparkConf["spark.dynamicAllocation.maxExecutors"]), if set. ParseDynamicAllocExecutorCount will return float64(0) if spec
value and sparkConf values are not set or if there is a parsing error.
*/
func ParseDynamicAllocExecutorCount(dynamicAllocationSpec *v1beta2.DynamicAllocation, sparkConf map[string]string) float64 {
	count := float64(0)
	var err error = nil
	if dynamicAllocationSpec != nil && dynamicAllocationSpec.MaxExecutors != nil { // set as --conf spark.dynamicAllocation.maxExecutors
		count = float64(*dynamicAllocationSpec.MaxExecutors)
	} else if val, ok := sparkConf["spark.dynamicAllocation.maxExecutors"]; ok {
		count, err = strconv.ParseFloat(val, 64)
		if err != nil {
			klog.Error(err)
		}
	}
	return count
}

/*
ParseExecutorCount will return the number of executors allocated based on Executor Instances config. Executor Instances
spec value (specInstance arg), if set, will take precedence over the SparkConf value
(sparkConf["spark.executor.instances"]), if set. ParseExecutorCount will return float64(1) if spec value and sparkConf
values are not set or if there is a parsing error.
*/
func ParseExecutorCount(specInstance *int32, sparkConf map[string]string) float64 {
	count := float64(1) // Spark Operator defaults to 1
	var err error = nil
	if specInstance != nil {
		count = float64(*specInstance) // spark.executor.instances
	} else if val, ok := sparkConf["spark.executor.instances"]; ok {
		count, err = strconv.ParseFloat(val, 64)
		if err != nil {
			klog.Error(err)
			count = float64(1)
		}
	}
	return count
}

/*
IsDynamicAllocationEnabled will use the dynamicAllocation spec and sparkConf to determine if dynamicAllocation is
enabled. It will return true if enabled, false if disabled, and false if there is a parsing error.
*/
func IsDynamicAllocationEnabled(dynamicAllocationSpec *v1beta2.DynamicAllocation, sparkConf map[string]string) bool {
	enabled := false
	var err error = nil

	if dynamicAllocationSpec != nil && dynamicAllocationSpec.Enabled {
		return true
	}
	if val, ok := sparkConf["spark.dynamicAllocation.enabled"]; ok {
		enabled, err = strconv.ParseBool(val)
		if err != nil {
			klog.Error(err)
		}
	}
	return enabled
}

/*
ParseKubeCPU takes the Kube CPU Units in string type and returns CPU Cores in float64 type.
*/
func ParseKubeCPU(cpuStr string) (float64, error) {
	if cpuStr == "" {
		return 0, nil
	}
	if strings.HasSuffix(cpuStr, "m") {
		millis, err := strconv.ParseFloat(strings.TrimSuffix(cpuStr, "m"), 64)
		if err != nil {
			return 0, err
		}
		return millis / 1000.0, nil
	}
	return strconv.ParseFloat(cpuStr, 64)
}
