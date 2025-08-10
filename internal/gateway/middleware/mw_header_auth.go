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
	"regexp"

	"github.com/gin-gonic/gin"
)

// HeaderAuthMiddleware will set the `user` context variable based on a desired list of Headers,
// if they are present within the context. Each desired header can pass a Validation regex to ensure
// it conforms to requirements set by Spark Gateway admins. The `user` context variable is set to the
// first header that passes it's configured Validation, if any.
type HeaderAuthMiddleware struct {
	Headers []HeaderAuthHeader
}

type HeaderAuthHeader struct {
	Key        string `koanf:"key"`
	Validation string `koanf:"validation"`
}

type HeaderAuthMiddlewareConf struct {
	Headers []HeaderAuthHeader `koanf:"headers"`
}

func (h *HeaderAuthMiddlewareConf) Validate() error {
	for _, authHeader := range h.Headers {
		if authHeader.Validation != "" {
			if _, err := regexp.Compile(authHeader.Validation); err != nil {
				return fmt.Errorf("invalid Validation regex for HeaderAuthHeader [%s]: %w", authHeader.Key, err)
			}
		}
	}

	return nil
}

func (h *HeaderAuthMiddlewareConf) Name() string {
	return "HeaderAuthMiddlewareConf"
}

func NewHeaderAuthMiddleware(confMap MiddlewareConfMap) (GatewayMiddleware, error) {
	var mwConf HeaderAuthMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, confMap); err != nil {
		return nil, fmt.Errorf("error loading %s config: %w", mwConf.Name(), err)
	}

	return &HeaderAuthMiddleware{Headers: mwConf.Headers}, nil
}

func (h *HeaderAuthMiddleware) Config(conf MiddlewareConfMap) error {

	return nil
}

func (h *HeaderAuthMiddleware) Handler(c *gin.Context) {

	for _, authHeader := range h.Headers {
		if gotHeader := c.GetHeader(authHeader.Key); gotHeader != "" {
			validateReg := regexp.MustCompile(authHeader.Validation)

			if validateMatch := validateReg.MatchString(gotHeader); validateMatch {
				c.Set("user", gotHeader)
				c.Next()
				return
			}
		}
	}

	c.Next()

}
