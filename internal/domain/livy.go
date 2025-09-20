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

package domain

import (
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const LIVY_BATCH_ID_LABEL string = "spark-gateway/livy-batch-id"

type LivySessionState int

const (
	LivySessionStateNotStarted LivySessionState = iota
	LivySessionStateStarting
	LivySessionStateIdle
	LivySessionStateBusy
	LivySessionStateShuttingDown
	LivySessionStateError
	LivySessionStateDead
	LivySessionStateKilled
	LivySessionStateSuccess
)

var sessionStateName = map[LivySessionState]string{
	LivySessionStateNotStarted:   "not_started",
	LivySessionStateStarting:     "started",
	LivySessionStateIdle:         "idle",
	LivySessionStateBusy:         "busy",
	LivySessionStateShuttingDown: "shutting_down",
	LivySessionStateError:        "error",
	LivySessionStateDead:         "dead",
	LivySessionStateKilled:       "killed",
	LivySessionStateSuccess:      "success",
}

var applicationTypeToSessionStateName = map[v1beta2.ApplicationStateType]LivySessionState{
	v1beta2.ApplicationStateNew:              LivySessionStateNotStarted,
	v1beta2.ApplicationStateSubmitted:        LivySessionStateStarting,
	v1beta2.ApplicationStateRunning:          LivySessionStateBusy,
	v1beta2.ApplicationStateCompleted:        LivySessionStateSuccess,
	v1beta2.ApplicationStateFailed:           LivySessionStateError,
	v1beta2.ApplicationStateFailedSubmission: LivySessionStateDead,
	v1beta2.ApplicationStatePendingRerun:     LivySessionStateDead,
	v1beta2.ApplicationStateInvalidating:     LivySessionStateShuttingDown,
	v1beta2.ApplicationStateSucceeding:       LivySessionStateShuttingDown,
	v1beta2.ApplicationStateFailing:          LivySessionStateShuttingDown,
	v1beta2.ApplicationStateUnknown:          LivySessionStateDead,
}

func (ss LivySessionState) String() string {
	return sessionStateName[ss]
}

func FromV1Beta2ApplicationState(state v1beta2.ApplicationStateType) LivySessionState {
	return applicationTypeToSessionStateName[state]
}

type LivyBatch struct {
	Id      int32             `json:"id"`
	AppId   string            `json:"appId"`
	AppInfo map[string]string `json:"appInfo"`
	TTL     string            `json:"ttl"`
	Log     []string          `json:"log"`
	State   string            `json:"state"`
}

type LivyCreateBatchRequest struct {
	File           string            `json:"file"`
	ProxyUser      string            `json:"proxyUser"`
	ClassName      string            `json:"className"`
	Args           []string          `json:"args"`
	Jars           []string          `json:"jars"`
	PyFiles        []string          `json:"pyFiles"`
	Files          []string          `json:"files"`
	DriverMemory   string            `json:"driverMemory"`
	DriverCores    int               `json:"driverCores"`
	ExecutorMemory string            `json:"executorMemory"`
	ExecutorCores  int               `json:"executorCores"`
	NumExecutors   int               `json:"numExecutors"`
	Archives       []string          `json:"archives"`
	Queue          string            `json:"queue"`
	Name           string            `json:"name"`
	Conf           map[string]string `json:"conf"`
}

func (c *LivyCreateBatchRequest) ToV1Beta2SparkApplication(namespace string) *v1beta2.SparkApplication {

	var appType v1beta2.SparkApplicationType
	if strings.HasSuffix(c.File, ".py") {
		appType = v1beta2.SparkApplicationTypePython
	} else {
		appType = v1beta2.SparkApplicationTypeJava
	}

	driverCores := int32(c.DriverCores)
	driverCoresLimit := strconv.Itoa(c.DriverCores)
	executorCores := int32(c.ExecutorCores)
	executorCoresLimit := strconv.Itoa(c.DriverCores)

	return &v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      c.Name,
			Namespace: namespace,
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                appType,
			Mode:                "cluster",
			MainClass:           &c.ClassName,
			MainApplicationFile: &c.File,
			Arguments:           c.Args,
			SparkConf:           c.Conf,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       &driverCores,
					CoreLimit:   &driverCoresLimit,
					Memory:      &c.DriverMemory,
					MemoryLimit: &c.DriverMemory,
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       &executorCores,
					CoreLimit:   &executorCoresLimit,
					Memory:      &c.ExecutorMemory,
					MemoryLimit: &c.ExecutorMemory,
				},
			},
			Deps: v1beta2.Dependencies{
				Jars:     c.Jars,
				Files:    c.Files,
				PyFiles:  c.PyFiles,
				Archives: c.Archives,
			},
			SparkVersion: "3",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
}

type LivyListBatchesResponse struct {
	From     int          `json:"from"`
	Total    int          `json:"total"`
	Sessions []*LivyBatch `json:"sessions"`
}

type LivyLogBatchResponse struct {
	Id   int      `json:"id"`
	From int      `json:"from"`
	Size int      `json:"size"`
	Log  []string `json:"log"`
}

type LivyGetBatchStateResponse struct {
	Id    int    `json:"id"`
	State string `json:"state"`
}
