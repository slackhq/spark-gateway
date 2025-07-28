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

const ServiceTokenMapPath = "/conf/service-auth-config.yaml"

// ServiceTokenAuth does validation based on specific headers sent through the request.
type ServiceTokenAuthMiddleware struct {
	Conf ServiceTokenAuthMiddlewareConf
}

type ServiceTokenAuthMiddlewareConf struct {
	ServiceTokenMapPath string `koanf:"serviceTokenMapFile"`
	ServiceTokenMap     map[string]string
}

func (s *ServiceTokenAuthMiddlewareConf) Name() string {
	return "ServiceTokenAuthMiddlewareConf"
}

// Unmarshal takes takes the loaded koanf of the service tokens map, so
// this will unmarshal to the map struct vs the ServiceTokenAuthMiddlewareConf itself
func (s *ServiceTokenAuthMiddlewareConf) Unmarshal(k *koanf.Koanf) error {
	serviceTokenMap := make(map[string]string)
	if err := k.Unmarshal(s.Key(), &serviceTokenMap); err != nil {
		return fmt.Errorf("error unmarshaling ServiceTokenAuthMiddlewareConf: %w", err)
	}

	s.ServiceTokenMap = serviceTokenMap

	return nil
}

// ServiceTokenAuthMiddlewareConf reads the service auth tokens file, so it needs
// the whole path to be able to map to ServiceTokenMap
func (s *ServiceTokenAuthMiddlewareConf) Key() string {
	return ""
}

func (s *ServiceTokenAuthMiddlewareConf) Validate() error {
	return nil
}

func NewServiceTokenAuthMiddleware() GatewayMiddleware {
	return &ServiceTokenAuthMiddleware{Conf: ServiceTokenAuthMiddlewareConf{
		ServiceTokenMapPath: ServiceTokenMapPath,
	}}
}

func (s *ServiceTokenAuthMiddleware) Name() string {
	return "ServiceTokenAuthMiddleware"
}

func (s *ServiceTokenAuthMiddleware) Config(conf MiddlewareConfMap) error {
	var mwConf ServiceTokenAuthMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, conf); err != nil {
		return fmt.Errorf("error loading %s config: %w", mwConf.Name(), err)
	}

	// If the loaded configuration has a file path and that path is different than the default,
	// set the Conf to the new loaded one
	if mwConf.ServiceTokenMapPath != "" && mwConf.ServiceTokenMapPath != s.Conf.ServiceTokenMapPath {
		s.Conf = mwConf
	}

	// New koanf to hold key/value pairs of Service tokens
	tokenK := koanf.New(".")
	if err := tokenK.Load(file.Provider(s.Conf.ServiceTokenMapPath), yaml.Parser()); err != nil {
		return fmt.Errorf("error parsing config file: %s", err)
	}

	// Unmarshal the key/values pairs into the ServiceTokenMap field
	if err := s.Conf.Unmarshal(tokenK); err != nil {
		return fmt.Errorf("error unmarshaling service token map file: %w", err)
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
	if gotToken, found := s.Conf.ServiceTokenMap[serviceName]; found {
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
