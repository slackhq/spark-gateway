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

package config

import (
	"fmt"

	"github.com/knadh/koanf/v2"
)

type ServiceTokenMap map[string]string

type GatewayServiceAuthConfig struct {
	Services ServiceTokenMap
}

// Unmarshal takes takes the loaded koanf of the service tokens map, so
// this will unmarshal to the map struct vs the GatewayServiceAuthConfig itself
func (g *GatewayServiceAuthConfig) Unmarshal(k *koanf.Koanf) error {
	serviceTokenMap := make(ServiceTokenMap)
	if err := k.Unmarshal(g.Key(), serviceTokenMap); err != nil {
		return fmt.Errorf("error unmarshaling GatewayConfig: %w", err)
	}

	g.Services = serviceTokenMap

	return nil
}

// GatewayServiceAuthConfig reads the service auth tokens file, so it needs
// the whole path to be able to map to ServiceTokenMap
func (g *GatewayServiceAuthConfig) Key() string {
	return ""
}
