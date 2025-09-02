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
	"regexp"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)

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
	allowRes       []string
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
		test:           "Fail regex (deny)",
		allowRes:       []string{"valid"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
	{
		test:           "Pass regex (allow)",
		allowRes:       []string{"test"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
		expectedUser:   "test",
	},
	{
		test:           "Multiple regex (deny)",
		allowRes:       []string{"one", "two"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
	{
		test:           "Multiple regex (allow)",
		allowRes:       []string{"one", "test"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
		expectedUser:   "test",
	},
}

func TestRegexAllowBasicAuthMiddleware(t *testing.T) {

	for _, test := range allowAuthHandlerTests {

		allowRegexes := []*regexp.Regexp{}
		for _, allowRe := range test.allowRes {
			allowRegexes = append(allowRegexes, regexp.MustCompile(allowRe))
		}

		mw := RegexBasicAuthAllowMiddleware{
			AllowRegexes: allowRegexes,
		}

		t.Run(test.test, func(t *testing.T) {

			router := gin.New()

			router.Use(mw.Handler)

			// User check
			if test.expectedUser != "" {
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

			assert.Equal(t, test.expectedStatus, w.Code)
		})
	}
}

var denyAuthHandlerTests = []struct {
	test           string
	denyRes        []string
	authHeader     string
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
		test:           "No matches (allow)",
		denyRes:        []string{"valid"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
	},
	{
		test:           "Match regex (deny)",
		denyRes:        []string{"test"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
	{
		test:           "Multiple regex (allow)",
		denyRes:        []string{"one", "two"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 200,
	},
	{
		test:           "Multiple regex (deny)",
		denyRes:        []string{"one", "test"},
		authHeader:     "Authorization Basic dGVzdDp0ZXN0",
		expectedStatus: 403,
	},
}

func TestRegexDenyBasicAuthMiddleware(t *testing.T) {

	for _, test := range denyAuthHandlerTests {

		denyRegexes := []*regexp.Regexp{}
		for _, denyRe := range test.denyRes {
			denyRegexes = append(denyRegexes, regexp.MustCompile(denyRe))
		}

		mw := RegexBasicAuthDenyMiddleware{
			DenyRegexes: denyRegexes,
		}

		t.Run(test.test, func(t *testing.T) {
			router := gin.New()

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
