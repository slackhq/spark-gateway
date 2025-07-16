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
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestServiceAuthConfigNoInput(t *testing.T) {

	s := NewServiceTokenAuthMiddleware().(*ServiceTokenAuthMiddleware)

	err := s.Config(MiddlewareConfMap{})

	// This should error because default is /etc/conf/ file which we don't want make for testing
	assert.NotNil(t, err, "err should exist")
	assert.Equal(t, ServiceTokenMapPath, s.Conf.ServiceTokenMapPath, "conf should still be default")
}

func TestServiceAuthConfigDifferentPath(t *testing.T) {

	tmpFile, err := os.CreateTemp("", "config.yaml")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configYaml := `service: token`

	if _, err := tmpFile.Write([]byte(configYaml)); err != nil {
		t.Fatalf("unable to write to temp file: %v", err)
	}
	tmpFile.Close()

	s := NewServiceTokenAuthMiddleware().(*ServiceTokenAuthMiddleware)

	err = s.Config(MiddlewareConfMap{
		"serviceTokenMapFile": tmpFile.Name(),
	})

	assert.Nil(t, err, "err should exist")
	assert.Equal(t, tmpFile.Name(), s.Conf.ServiceTokenMapPath, "conf should still be default")
	assert.Equal(t, map[string]string{"service": "token"}, s.Conf.ServiceTokenMap, "service token map should be equal")
}

type header struct {
	Name  string
	Value string
}

var testUser string = "test-user"
var testToken string = "test-token"

var services = map[string]string{
	testUser: testToken,
}

var serviceAuthValidateTests = []struct {
	test           string
	conf           ServiceTokenAuthMiddlewareConf
	headers        []header
	user           string
	expectedUser   string
	expectedStatus int
}{
	{
		test:           "missing headers",
		expectedStatus: 200,
	},
	{
		test: "missing token header",
		headers: []header{
			{Name: "X-Spark-Gateway-User", Value: "exists"},
		},
		expectedStatus: 401,
	},
	{
		test: "service not authorized",
		headers: []header{
			{Name: "X-Spark-Gateway-User", Value: "unauthorized-user"},
			{Name: "X-Spark-Gateway-Token", Value: "fake-token"},
		},
		expectedStatus: 403,
	},
	{
		test: "invalid token",
		headers: []header{
			{Name: "X-Spark-Gateway-User", Value: testUser},
			{Name: "X-Spark-Gateway-Token", Value: "fake-token"},
		},
		expectedStatus: 403,
	},
	{
		test: "valid header",
		conf: ServiceTokenAuthMiddlewareConf{
			ServiceTokenMap: services,
		},
		headers: []header{
			{Name: "X-Spark-Gateway-Token", Value: testToken},
			{Name: "X-Spark-Gateway-User", Value: testUser},
		},
		expectedUser:   testUser,
		expectedStatus: 200,
	},
}

func TestServiceTokenAuthMiddleware(t *testing.T) {

	for _, test := range serviceAuthValidateTests {

		mw := ServiceTokenAuthMiddleware{
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
			if test.headers != nil {
				for _, inHeader := range test.headers {
					req.Header.Add(inHeader.Name, inHeader.Value)
				}
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, test.expectedStatus, w.Code)
		})
	}
}
