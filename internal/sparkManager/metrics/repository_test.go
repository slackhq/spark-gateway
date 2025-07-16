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
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func int32Ptr(i int32) *int32 { return &i }
func strPtr(s string) *string { return &s }

func TestParseK8sCoresValue(t *testing.T) {
	tests := []struct {
		name         string
		specValue    *string
		sparkConf    map[string]string
		sparkConfKey string
		expected     float64
	}{
		{
			name:         "Spec value valid in milliCPU",
			specValue:    strPtr("1500m"),
			sparkConf:    nil,
			sparkConfKey: "unused",
			expected:     1.5,
		},
		{
			name:         "SparkConf value valid",
			specValue:    nil,
			sparkConf:    map[string]string{"spark.kubernetes.driver.request.cores": "2"},
			sparkConfKey: "spark.kubernetes.driver.request.cores",
			expected:     2.0,
		},
		{
			name:         "Both missing",
			specValue:    nil,
			sparkConf:    map[string]string{},
			sparkConfKey: "spark.kubernetes.driver.request.cores",
			expected:     0.0,
		},
		{
			name:         "Spec value invalid",
			specValue:    strPtr("invalid"),
			sparkConf:    nil,
			sparkConfKey: "unused",
			expected:     0.0,
		},
		{
			name:         "SparkConf value invalid",
			specValue:    nil,
			sparkConf:    map[string]string{"spark.kubernetes.driver.request.cores": "invalid"},
			sparkConfKey: "spark.kubernetes.driver.request.cores",
			expected:     0.0,
		},
	}
	for _, test := range tests {
		got := ParseK8sCoresValue(test.specValue, test.sparkConf, test.sparkConfKey)
		if got != test.expected {
			t.Errorf("%s: expected %v, got %v", test.name, test.expected, got)
		}
	}
}

func TestParseCoresValue(t *testing.T) {
	tests := []struct {
		name         string
		specValue    *int32
		sparkConf    map[string]string
		sparkConfKey string
		expected     float64
	}{
		{
			name:         "Spec value provided",
			specValue:    int32Ptr(2),
			sparkConf:    nil,
			sparkConfKey: "spark.driver.cores",
			expected:     2.0,
		},
		{
			name:         "SparkConf value provided",
			specValue:    nil,
			sparkConf:    map[string]string{"spark.driver.cores": "3.5"},
			sparkConfKey: "spark.driver.cores",
			expected:     3.5,
		},
		{
			name:         "Both missing",
			specValue:    nil,
			sparkConf:    map[string]string{},
			sparkConfKey: "spark.driver.cores",
			expected:     0.0,
		},
		{
			name:         "Invalid SparkConf value",
			specValue:    nil,
			sparkConf:    map[string]string{"spark.driver.cores": "invalid"},
			sparkConfKey: "spark.driver.cores",
			expected:     0.0,
		},
	}
	for _, test := range tests {
		got := ParseCoresValue(test.specValue, test.sparkConf, test.sparkConfKey)
		if got != test.expected {
			t.Errorf("%s: expected %v, got %v", test.name, test.expected, got)
		}
	}
}

func TestParseDynamicAllocExecutorCount(t *testing.T) {
	tests := []struct {
		name         string
		dynamicAlloc *v1beta2.DynamicAllocation
		sparkConf    map[string]string
		expected     float64
	}{
		{
			name:         "Spec provided",
			dynamicAlloc: &v1beta2.DynamicAllocation{MaxExecutors: int32Ptr(5)},
			sparkConf:    nil,
			expected:     5.0,
		},
		{
			name:         "SparkConf provided",
			dynamicAlloc: &v1beta2.DynamicAllocation{},
			sparkConf:    map[string]string{"spark.dynamicAllocation.maxExecutors": "7"},
			expected:     7.0,
		},
		{
			name:         "Neither provided",
			dynamicAlloc: &v1beta2.DynamicAllocation{},
			sparkConf:    map[string]string{},
			expected:     0.0,
		},
		{
			name:         "Invalid SparkConf value",
			dynamicAlloc: &v1beta2.DynamicAllocation{},
			sparkConf:    map[string]string{"spark.dynamicAllocation.maxExecutors": "invalid"},
			expected:     0.0,
		},
		{
			name:         "nil dynamicAllocation pointer",
			dynamicAlloc: nil,
			sparkConf:    map[string]string{},
			expected:     0.0,
		},
		{
			name:         "nil dynamicAllocation pointer and sparkConf provided",
			dynamicAlloc: nil,
			sparkConf:    map[string]string{"spark.dynamicAllocation.maxExecutors": "1000"},
			expected:     1000.0,
		},
	}
	for _, test := range tests {
		got := ParseDynamicAllocExecutorCount(test.dynamicAlloc, test.sparkConf)
		if got != test.expected {
			t.Errorf("%s: expected %v, got %v", test.name, test.expected, got)
		}
	}
}

func TestParseExecutorCount(t *testing.T) {
	tests := []struct {
		name         string
		specInstance *int32
		sparkConf    map[string]string
		expected     float64
	}{
		{
			name:         "Spec provided",
			specInstance: int32Ptr(3),
			sparkConf:    nil,
			expected:     3.0,
		},
		{
			name:         "SparkConf provided",
			specInstance: nil,
			sparkConf:    map[string]string{"spark.executor.instances": "4"},
			expected:     4.0,
		},
		{
			name:         "Neither provided",
			specInstance: nil,
			sparkConf:    map[string]string{},
			expected:     1.0,
		},
		{
			name:         "Invalid SparkConf value",
			specInstance: nil,
			sparkConf:    map[string]string{"spark.executor.instances": "invalid"},
			expected:     1.0,
		},
	}
	for _, test := range tests {
		got := ParseExecutorCount(test.specInstance, test.sparkConf)
		if got != test.expected {
			t.Errorf("%s: expected %v, got %v", test.name, test.expected, got)
		}
	}
}

func TestIsDynamicAllocationEnabled(t *testing.T) {
	tests := []struct {
		name         string
		dynamicAlloc *v1beta2.DynamicAllocation
		sparkConf    map[string]string
		expected     bool
	}{
		{
			name:         "Spec enabled",
			dynamicAlloc: &v1beta2.DynamicAllocation{Enabled: true},
			sparkConf:    nil,
			expected:     true,
		},
		{
			name:         "Spec disabled but sparkConf true",
			dynamicAlloc: &v1beta2.DynamicAllocation{Enabled: false},
			sparkConf:    map[string]string{"spark.dynamicAllocation.enabled": "true"},
			expected:     true,
		},
		{
			name:         "Both false",
			dynamicAlloc: &v1beta2.DynamicAllocation{Enabled: false},
			sparkConf:    map[string]string{},
			expected:     false,
		},
		{
			name:         "Spec missing",
			dynamicAlloc: nil,
			sparkConf:    map[string]string{},
			expected:     false,
		},
		{
			name:         "Spec missing, sparkConf true",
			dynamicAlloc: nil,
			sparkConf:    map[string]string{"spark.dynamicAllocation.enabled": "true"},
			expected:     true,
		},
		{
			name:         "Invalid sparkConf value",
			dynamicAlloc: &v1beta2.DynamicAllocation{Enabled: false},
			sparkConf:    map[string]string{"spark.dynamicAllocation.enabled": "invalid"},
			expected:     false,
		},
	}
	for _, tc := range tests {
		got := IsDynamicAllocationEnabled(tc.dynamicAlloc, tc.sparkConf)
		if got != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, got)
		}
	}
}

func TestParseKubeCPU(t *testing.T) {
	tests := []struct {
		name        string
		cpuStr      string
		expected    float64
		expectError bool
	}{
		{
			name:        "Empty string returns zero",
			cpuStr:      "",
			expected:    0,
			expectError: false,
		},
		{
			name:        "milliCPU value - 1000m equals 1 core",
			cpuStr:      "1000m",
			expected:    1.0,
			expectError: false,
		},
		{
			name:        "milliCPU value - 1500m equals 1.5 cores",
			cpuStr:      "1500m",
			expected:    1.5,
			expectError: false,
		},
		{
			name:        "milliCPU value - 500m equals 0.5 core",
			cpuStr:      "500m",
			expected:    0.5,
			expectError: false,
		},
		{
			name:        "Plain numeric string - 2 equals 2 cores",
			cpuStr:      "2",
			expected:    2.0,
			expectError: false,
		},
		{
			name:        "Plain numeric string with decimal - 2.5 equals 2.5 cores",
			cpuStr:      "2.5",
			expected:    2.5,
			expectError: false,
		},
		{
			name:        "Invalid non-numeric string returns error",
			cpuStr:      "invalid",
			expected:    0,
			expectError: true,
		},
		{
			name:        "Invalid unit suffix returns error",
			cpuStr:      "2000x",
			expected:    0,
			expectError: true,
		},
		{
			name:        "milliCPU with missing number returns error",
			cpuStr:      "m",
			expected:    0,
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseKubeCPU(test.cpuStr)
			if test.expectError {
				if err == nil {
					t.Errorf("%s: expected error for input %q, got nil", test.name, test.cpuStr)
				}
			} else {
				if err != nil {
					t.Errorf("%s unexpected error for input %q: %v", test.name, test.cpuStr, err)
				}
				if result != test.expected {
					t.Errorf("%s: for cpuStr %q, expected %v, got %v", test.name, test.cpuStr, test.expected, result)
				}
			}
		})
	}
}
