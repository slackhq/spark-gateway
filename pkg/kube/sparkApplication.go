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

import "github.com/kubeflow/spark-operator/v2/api/v1beta2"

// Sanitize removes fields deemed unnecessary for client interactions
func Sanitize(sparkApp *v1beta2.SparkApplication) *v1beta2.SparkApplication {
	// Drop non-useful fields
	sparkApp.ObjectMeta.ManagedFields = nil
	sparkApp.Status.ExecutorState = nil
	return sparkApp
}
