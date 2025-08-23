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

package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const ServiceTokenMapPathDefault = "/conf/service-auth-config.yaml"

type ServiceTokenMap map[string]string

// ServiceTokenAuth does validation based on specific headers sent through the request.
type ServiceTokenAuthMiddleware struct {
	ServiceTokenMap ServiceTokenMap
}

type ServiceTokenAuthMiddlewareConf struct {
	ServiceTokenMapPath string `koanf:"serviceTokenMapFile"`
}

func (s *ServiceTokenAuthMiddlewareConf) Name() string {
	return "ServiceTokenAuthMiddlewareConf"
}

func (s *ServiceTokenAuthMiddlewareConf) Validate() error {
	return nil
}

func NewServiceTokenAuthMiddleware(confMap MiddlewareConfMap) (GatewayMiddleware, error) {
	var mwConf ServiceTokenAuthMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, confMap); err != nil {
		return nil, fmt.Errorf("error creating ServiceTokenAuthMiddleware: %w", err)
	}

	var serviceTokenMap ServiceTokenMap

	if err := LoadServiceTokensFromFile(&serviceTokenMap, mwConf); err != nil {
		return nil, fmt.Errorf("error creating ServiceTokenAuthMiddleware: %w", err)
	}

	return &ServiceTokenAuthMiddleware{ServiceTokenMap: serviceTokenMap}, nil
}

// LoadServiceTokensFromFile marshals the key/value pairs from the path provided in the passed ServiceTokenAuthMiddlewareConf with Koanf. If
// the provided config does not have a configured ServiceTokenMapPath, we use ServiceTokenMapPathDefault.
func LoadServiceTokensFromFile(tokenMap *ServiceTokenMap, mwConf ServiceTokenAuthMiddlewareConf) error {

	filePath := mwConf.ServiceTokenMapPath
	// If no path was provided in the Config, use a default
	if filePath == "" {
		filePath = ServiceTokenMapPathDefault
	}

	// New koanf to hold key/value pairs of Service tokens
	tokenK := koanf.New(".")
	if err := tokenK.Load(file.Provider(filePath), yaml.Parser()); err != nil {
		return fmt.Errorf("error parsing service token map file: %s", err)
	}

	// Unmarshal the key/values pairs into the ServiceTokenMap field
	if err := tokenK.Unmarshal("", tokenMap); err != nil {
		return fmt.Errorf("error unmarshaling ServiceTokenAuthMiddlewareConf: %w", err)
	}

	return nil
}

// Validate checks in the X-SparkGateway-Secret header is set.
func (s *ServiceTokenAuthMiddleware) Handler(c *gin.Context) {

	serviceName := c.GetHeader("X-Spark-Gateway-User")

	// No header, not using this middleware so we continue
	if serviceName == "" {
		c.Next()
		return
	}

	serviceToken := c.GetHeader("X-Spark-Gateway-Token")

	// If user set but no token, we refuse
	if serviceToken == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "X-Spark-Gateway-Token is not set"})
	}

	// Check if service is authorized
	if gotToken, found := s.ServiceTokenMap[serviceName]; found {
		if serviceToken == gotToken {
			c.Set("user", serviceName)
			c.Next()
			return
		} else {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": fmt.Sprintf("service %s not authorized: Invalid token", serviceName)})
			return
		}
	} else {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("service %s not authorized", serviceName)})
		return
	}

}
