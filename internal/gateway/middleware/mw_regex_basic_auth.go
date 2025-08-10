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
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

// GetUserFromAuthHeader ensures a valid Basic authorization header and returns the decoded username.
func GetUserFromAuthHeader(authHeader string) (string, error) {
	authToken := strings.Split(authHeader, "Basic ")

	if len(authToken) != 2 {
		return "", errors.New("invalid Authorization format, must be like `Basic <token>`")
	}

	decoded, err := base64.StdEncoding.DecodeString(authToken[1])

	if err != nil {
		return "", fmt.Errorf("could not decode auth token: %w", err)
	}

	userPass := strings.Split(string(decoded), ":")

	if len(userPass) != 2 {
		return "", fmt.Errorf("could not parse decoded auth token")
	}

	return userPass[0], nil
}

type RegexBasicAuthAllowMiddlewareConf struct {
	Allow []string `koanf:"allow"`
}

func (r *RegexBasicAuthAllowMiddlewareConf) Name() string {
	return "RegexBasicAuthAllowMiddlewareConf"
}

// Validate ensures all regexes in RegexBasicAuthAllowMiddlewareConf are valid
func (r *RegexBasicAuthAllowMiddlewareConf) Validate() error {
	for _, allowReg := range r.Allow {
		if _, err := regexp.Compile(allowReg); err != nil {
			return fmt.Errorf("invalid allow regex: %w", err)
		}
	}

	return nil
}

// RegexBasicAuthAllowMiddleware matches the Basic Authorization token against an
// allow list of regexes. Will set the context `user` key if the passed
// basic auth satisfies the allow/deny list criteria. If auth header is missing,
// the request is denied.
type RegexBasicAuthAllowMiddleware struct {
	AllowRegexes []*regexp.Regexp
}

func NewRegexBasicAuthAllowMiddleware(confMap MiddlewareConfMap) (GatewayMiddleware, error) {

	var mwConf RegexBasicAuthAllowMiddlewareConf
	if err := LoadMiddlewareConf(&mwConf, confMap); err != nil {
		return nil, fmt.Errorf("error creating RegexBasicAuthAllowMiddleware: %w", err)
	}

	// can use MustCompile because of Validate call earlier in LoadMiddlewareConf
	allowRegexes := []*regexp.Regexp{}
	for _, allowRegexString := range mwConf.Allow {
		allowRegex := regexp.MustCompile(allowRegexString)
		allowRegexes = append(allowRegexes, allowRegex)
	}

	return &RegexBasicAuthAllowMiddleware{AllowRegexes: allowRegexes}, nil
}

func (r *RegexBasicAuthAllowMiddleware) Handler(c *gin.Context) {

	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		authUser, err := GetUserFromAuthHeader(authHeader)

		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid Authorization header"})
			return
		}

		if allowUser := r.AllowUsername(authUser); allowUser {
			c.Set("user", authUser)
		} else {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "user is unauthorized"})
		}
	}

	c.Next()
}

// AllowUsername will use the regexes defined in RegexBasicAuthAllowMiddlewareConf to determine
// whether a user is authorized or not.
func (r *RegexBasicAuthAllowMiddleware) AllowUsername(username string) bool {

	for _, allowRegex := range r.AllowRegexes {
		if allowMatch := allowRegex.MatchString(username); allowMatch {
			return true
		}
	}

	return false

}

type RegexBasicAuthDenyMiddlewareConf struct {
	Deny []string `koanf:"deny"`
}

func (r *RegexBasicAuthDenyMiddlewareConf) Name() string {
	return "RegexBasicAuthDenyMiddlewareConf"
}

// Validate ensures all regexes in RegexBasicAuthDenyMiddlewareConf are valid
func (r *RegexBasicAuthDenyMiddlewareConf) Validate() error {
	for _, denyReg := range r.Deny {
		if _, err := regexp.Compile(denyReg); err != nil {
			return fmt.Errorf("invalid allow regex: %w", err)
		}
	}

	return nil
}

// RegexBasicAuthDenyMiddleware matches the Basic Authorization token against an
// deny list of regexes. If a match is found, the request is denied.
type RegexBasicAuthDenyMiddleware struct {
	DenyRegexes []*regexp.Regexp
}

func NewRegexBasicAuthDenyMiddleware(confMap MiddlewareConfMap) (GatewayMiddleware, error) {

	var mwConf RegexBasicAuthDenyMiddlewareConf
	if err := LoadMiddlewareConf(&mwConf, confMap); err != nil {
		return nil, fmt.Errorf("error creating RegexBasicAuthDenyMiddleware: %w", err)
	}

	// can use MustCompile because of Validate call earlier in LoadMiddlewareConf
	denyRegexes := []*regexp.Regexp{}
	for _, denyRegexString := range mwConf.Deny {
		denyRegex := regexp.MustCompile(denyRegexString)
		denyRegexes = append(denyRegexes, denyRegex)
	}

	return &RegexBasicAuthDenyMiddleware{DenyRegexes: denyRegexes}, nil
}

func (r *RegexBasicAuthDenyMiddleware) Name() string {
	return "RegexBasicAuthDenyMiddleware"
}

func (r *RegexBasicAuthDenyMiddleware) Handler(c *gin.Context) {

	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		authUser, err := GetUserFromAuthHeader(authHeader)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid Authorization header"})
			return
		}

		if denyUser := r.DenyUsername(authUser); denyUser {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "user is unauthorized"})
		}

	}

	c.Next()
}

// DenyUsername will use the regexes defined in RegexBasicAuthDenyMiddlewareConf to determine
// whether a user is authorized. If a match is found, user is denied access, else
// we consider them allowed.
func (r *RegexBasicAuthDenyMiddleware) DenyUsername(username string) bool {

	// check deny regexes first
	for _, denyRegex := range r.DenyRegexes {
		if denyMatch := denyRegex.MatchString(username); denyMatch {
			return true
		}
	}

	return false

}
