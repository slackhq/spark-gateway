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

package livy

import (
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/domain"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const LIVY_BATCH_ID_LABEL string = "spark-gateway/livy-batch-id"

type SessionState int

const (
	NotStarted SessionState = iota
	Starting
	Idle
	Busy
	ShuttingDown
	Error
	Dead
	Killed
	Success
)

var sessionStateName = map[SessionState]string{
	NotStarted:   "not_started",
	Starting:     "started",
	Idle:         "idle",
	Busy:         "busy",
	ShuttingDown: "shutting_down",
	Error:        "error",
	Dead:         "dead",
	Killed:       "killed",
	Success:      "success",
}

func (ss SessionState) String() string {
	return sessionStateName[ss]
}

type Batch struct {
	Id      int               `json:"id"`
	AppId   string            `json:"appId"`
	AppInfo map[string]string `json:"appInfo"`
	TTL     string            `json:"ttl"`
	Log     []string          `json:"log"`
	State   string            `json:"state"`
}

type CreateBatchRequest struct {
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

func (c *CreateBatchRequest) ToLivyBatch(id int, gsa domain.GatewaySparkApplication) *Batch {
	return &Batch{
		Id:      id,
		AppId:   "",
		AppInfo: map[string]string{},
		Log:     []string{},
		State:   "" ,
	}
}

func (c *CreateBatchRequest) ToV1Beta2SparkApplication(id int32, namespace string) *v1beta2.SparkApplication {

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
			Labels: map[string]string{
				LIVY_BATCH_ID_LABEL: strconv.Itoa(int(id)),
			},
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

type ListBatchRequest struct {
	From int `json:"from"`
	Size int `json:"size"`
}

type LogBatchRequest struct {
	From int `json:"from"`
	Size int `json:"size"`
}

type LogBatchResponse struct {
	Id   int      `json:"id"`
	From int      `json:"from"`
	Size int      `json:"size"`
	Log  []string `json:"log"`
}

type GetBatchStateResponse struct {
	Id    string `json:"id"`
	State string `json:"state"`
}
