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

	"github.com/gin-gonic/gin"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/v2"
)

type MiddlewareConfMap map[string]interface{}
type NewMiddleware func(conf MiddlewareConfMap) (GatewayMiddleware, error)

var BuiltinMiddleware map[string]NewMiddleware = map[string]NewMiddleware{
	"RegexBasicAuthAllowMiddleware": NewRegexBasicAuthAllowMiddleware,
	"RegexBasicAuthDenyMiddleware":  NewRegexBasicAuthDenyMiddleware,
	"HeaderAuthMiddleware":          NewHeaderAuthMiddleware,
	"ServiceTokenAuthMiddleware":    NewServiceTokenAuthMiddleware,
}

//go:generate moq -out mockmiddleware.go . GatewayMiddleware

type GatewayMiddleware interface {

	// Handler returns the actual Gin HandlerFunc used in the chain
	Handler(c *gin.Context)
}

//go:generate moq -out mockmiddlewareconf.go . GatewayMiddlewareConf

type GatewayMiddlewareConf interface {
	Name() string
	Validate() error
}

func LoadMiddlewareConf(mw GatewayMiddlewareConf, conf MiddlewareConfMap) error {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(conf, ""), nil); err != nil {
		return fmt.Errorf("error loading %s config: %w", mw.Name(), err)
	}

	if err := k.Unmarshal("", &mw); err != nil {
		return fmt.Errorf("error unmarshaling %s: %w", mw.Name(), err)
	}

	if err := mw.Validate(); err != nil {
		return fmt.Errorf("error validating %s: %w", mw.Name(), err)
	}

	return nil
}
