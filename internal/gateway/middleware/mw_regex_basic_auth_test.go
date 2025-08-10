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

var test_str string = "test"

var basicAuthGetUserTests = []struct {
	test       string
	authHeader string
	expected   string
	errMsg     string
}{
	{
		test:       "auth header missing",
		authHeader: "",
		errMsg:     "invalid Authorization format, must be like `Basic <token>`",
	},
	{
		test:       "bad decoding",
		authHeader: "Authorization Basic decode%%%",
		errMsg:     "could not decode auth token:",
	},
	{
		test:       "valid auth header",
		authHeader: "Authorization Basic dGVzdDp0ZXN0",
		expected:   "test",
	},
}

var allowAuthConfValidateTests = []struct {
	test  string
	allow []string
	err   string
}{
	{
		test:  "No regexes",
		allow: []string{},
		err:   "",
	},
	{
		test: "Valid regex",
		allow: []string{
			".*",
		},
		err: "",
	},
	{
		test: "Invalid regex",
		allow: []string{
			"good", "*",
		},
		err: "invalid allow regex: ",
	},
}

func TestAllowRegexBasicAuthConfValidate(t *testing.T) {
	for _, test := range allowAuthConfValidateTests {
		conf := RegexBasicAuthAllowMiddlewareConf{
			Allow: test.allow,
		}
		t.Run(test.test, func(t *testing.T) {

			err := conf.Validate()
			if err != nil {
				assert.Contains(t, err.Error(), test.err, "err message matches")
			}
		})
	}
}

var allowAuthHandlerTests = []struct {
	test           string
	conf           RegexBasicAuthAllowMiddlewareConf
	user           string
	authHeader     string
	expectedUser   string
	expectedStatus int
}{
	{
		test:           "Missing token",
		expectedStatus: 200,
	},
	{
		test:           "Bad token",
		authHeader:     "bad",
		expectedStatus: 401,
	},
	{
		test:           "Empty conf",
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
	{
		test: "Fail regex",
		conf: RegexBasicAuthAllowMiddlewareConf{
			Allow: []string{"valid"},
		},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
	{
		test: "Pass regex",
		conf: RegexBasicAuthAllowMiddlewareConf{
			Allow: []string{"test"},
		},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
		expectedUser:   "test",
	},
}

func TestRegexAllowBasicAuthMiddleware(t *testing.T) {

	for _, test := range allowAuthHandlerTests {

		mw := RegexBasicAuthAllowMiddleware{
			Conf: test.conf,
		}

		t.Run(test.test, func(t *testing.T) {

			router := gin.New()

			router.Use(func(c *gin.Context) {
				c.Set("user", test.user)
				c.Next()
			})

			router.Use(mw.Handler)

			// User check
			router.Use(func(ctx *gin.Context) {
				userVal, _ := ctx.Get("user")

				assert.Equal(t, test.expectedUser, userVal, "user value should match")
			})

			router.GET("/")

			req, _ := http.NewRequest("GET", "/", nil)
			if test.authHeader != "" {
				req.Header.Add("Authorization", test.authHeader)
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, test.expectedStatus, w.Code)
		})
	}
}

var denyAuthHandlerTests = []struct {
	test           string
	conf           RegexBasicAuthDenyMiddlewareConf
	authHeader     string
	user           string
	expectedStatus int
}{
	{
		test:           "Missing token",
		expectedStatus: 200,
	},
	{
		test:           "Bad token",
		authHeader:     "bad",
		expectedStatus: 401,
	},
	{
		test:           "Empty conf",
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
	},
	{
		test: "Fail regex",
		conf: RegexBasicAuthDenyMiddlewareConf{
			Deny: []string{"valid"},
		},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
	},
	{
		test: "Pass regex",
		conf: RegexBasicAuthDenyMiddlewareConf{
			Deny: []string{"test"},
		},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
}

func TestRegexDenyBasicAuthMiddleware(t *testing.T) {

	for _, test := range denyAuthHandlerTests {

		mw := RegexBasicAuthDenyMiddleware{
			Conf: test.conf,
		}

		t.Run(test.test, func(t *testing.T) {
			router := gin.New()

			router.Use(func(c *gin.Context) {
				c.Set("user", test.user)
				c.Next()
			})

			router.Use(mw.Handler)

			router.GET("/")

			req, _ := http.NewRequest("GET", "/", nil)
			if test.authHeader != "" {
				req.Header.Add("Authorization", test.authHeader)
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, test.expectedStatus, w.Code)
		})
	}
}
