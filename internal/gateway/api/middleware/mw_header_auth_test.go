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
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
}

var validateTests = []struct {
	test       string
	key        string
	validation string
	err        string
}{
	{
		"No Validation",
		"Header",
		"",
		"",
	},
	{
		"Valid validation",
		"Header",
		".*",
		"",
	},
	{
		"Bad regex",
		"BadHeader",
		"*",
		"invalid Validation regex for HeaderAuthHeader [BadHeader]:",
	},
}

func TestHeaderAuthMiddlewareConfValidate(t *testing.T) {
	for _, test := range validateTests {

		conf := HeaderAuthMiddlewareConf{
			Headers: []HeaderAuthHeader{
				{
					Key:        test.key,
					Validation: test.validation,
				},
			},
		}

		t.Run(test.test, func(t *testing.T) {
			err := conf.Validate()

			if err != nil {
				assert.Contains(t, err.Error(), test.err, "errors should match")
			} else {
				assert.Equal(t, "", test.err, "should be no error")
			}

		})
	}
}

var handlerValidTests = []struct {
	test         string
	authHeaders  []HeaderAuthHeader
	reqHeader    string
	headerVal    string
	user         string
	expectedUser string
	expectedSig  string
}{
	{
		test:         "User exists",
		authHeaders:  []HeaderAuthHeader{},
		user:         "exists",
		expectedUser: "exists",
		expectedSig:  "AB",
	},
	{
		test:        "No auth headers",
		authHeaders: []HeaderAuthHeader{},
		reqHeader:   "Header",
		headerVal:   "headerUser",
		expectedSig: "AB",
	},
	{
		test: "No user, no validation",
		authHeaders: []HeaderAuthHeader{
			{
				Key: "Header",
			},
		},
		reqHeader:    "Header",
		headerVal:    "headerUser",
		expectedUser: "headerUser",
		expectedSig:  "AB",
	},
	{
		test: "No user, validation",
		authHeaders: []HeaderAuthHeader{
			{
				Key:        "Header",
				Validation: ".*",
			},
		},
		reqHeader:    "Header",
		headerVal:    "headerUser",
		expectedUser: "headerUser",
		expectedSig:  "AB",
	},
	{
		test: "No user, invalid header",
		authHeaders: []HeaderAuthHeader{
			{
				Key:        "Header",
				Validation: "mustmatch",
			},
		},
		reqHeader:   "Header",
		headerVal:   "headerUser",
		expectedSig: "AB",
	},
}

func TestHeaderAuthMiddleware(t *testing.T) {

	for _, test := range handlerValidTests {

		mw := HeaderAuthMiddleware{
			Headers: test.authHeaders,
		}

		t.Run(test.test, func(t *testing.T) {

			signature := ""
			router := gin.New()

			router.Use(func(c *gin.Context) {
				signature += "A"
				c.Set("user", test.user)
				c.Next()
				signature += "B"
			})

			router.Use(mw.Handler)

			// User check
			router.Use(func(ctx *gin.Context) {
				userVal, _ := ctx.Get("user")

				assert.Equal(t, test.expectedUser, userVal, "user value should match")
			})

			router.GET("/")

			req, _ := http.NewRequest("GET", "/", nil)
			req.Header.Add(test.reqHeader, test.headerVal)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, test.expectedSig, signature)
		})
	}
}
