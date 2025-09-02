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

package clusterrouter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWeightedRandomChooseClusterID(t *testing.T) {
	clusterAWeight := 5.0
	clusterBWeight := 15.0
	metricsClusterA := metric{
		Weight: clusterAWeight,
	}
	metricsClusterB := metric{
		Weight: clusterBWeight,
	}
	totalWeight := clusterAWeight + clusterBWeight
	weightsMap := map[string]*metric{
		"clustera": &metricsClusterA,
		"clusterb": &metricsClusterB,
	}
	tests := []struct {
		randomInt         int
		expectedClusterID string
	}{
		{
			randomInt:         0,
			expectedClusterID: "clustera",
		},
		{
			randomInt:         4,
			expectedClusterID: "clustera",
		},
		{
			randomInt:         5,
			expectedClusterID: "clusterb",
		},
		{
			randomInt:         19,
			expectedClusterID: "clusterb",
		},
	}
	for _, test := range tests {
		assert.Less(t, test.randomInt, int(totalWeight), "rand.Intn(int(*totalWeight)) will always generate random int less than totalWeight")
		chosenClusterId := chooseClusterIDRandom(weightsMap, test.randomInt)
		assert.Equal(t, test.expectedClusterID, chosenClusterId, "ClusterID should be equal for random it %v", test.randomInt)
	}
}
