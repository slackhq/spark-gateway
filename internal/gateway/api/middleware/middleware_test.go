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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
}

var addMiddlewareTest = []struct {
	test         string
	mwDefs       []config.MiddlewareDefinition
	authHeader   string
	expectedUser string
	err          string
}{
	{
		"No Middlewares",
		[]config.MiddlewareDefinition{},
		"",
		"anonymous",
		"",
	},
	{
		"No Middleware Type",
		[]config.MiddlewareDefinition{
			{
				Type: "bad",
			},
		},
		"",
		"",
		"no builtin middleware with type [bad]",
	},
	{
		"Configured Middleware",
		[]config.MiddlewareDefinition{
			{
				Type: "RegexBasicAuthAllowMiddleware",
				Conf: map[string]any{
					"allow": []string{".*"},
				},
			},
		},
		"Authorization Basic dGVzdDp0ZXN0",
		"test",
		"",
	},
}

func TestAddMiddleware(t *testing.T) {

	for _, test := range addMiddlewareTest {

		t.Run(test.test, func(t *testing.T) {
			router := gin.New()

			mwChain, err := AddMiddleware(test.mwDefs)
			if err != nil {
				assert.Equal(t, test.err, err.Error(), "errors should match")
				return
			}

			router.Use(mwChain...)

			if test.expectedUser != "" {
				// User check
				router.Use(func(ctx *gin.Context) {
					userVal, _ := ctx.Get("user")

					assert.Equal(t, test.expectedUser, userVal, "user value should match")
				})

			}

			router.GET("/")

			req, _ := http.NewRequest("GET", "/", nil)
			if test.authHeader != "" {
				req.Header.Add("Authorization", test.authHeader)
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}
