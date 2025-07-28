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

// BaseRegexBasicAuthMiddleware matches the Basic Authorization token against an
// list of regexes
type BaseRegexBasicAuthMiddleware struct {
	Conf BaseRegexBasicAuthMiddlewareConf
}

type BaseRegexBasicAuthMiddlewareConf struct{}

func (bc *BaseRegexBasicAuthMiddlewareConf) Name() string {
	return "BaseRegexBasicAuthMiddlewareConf"
}

func (b *BaseRegexBasicAuthMiddlewareConf) Validate() error {
	return nil
}

func NewBaseRegexBasicAuthMiddleware() GatewayMiddleware {
	return &BaseRegexBasicAuthMiddleware{}
}

func (b *BaseRegexBasicAuthMiddleware) Config(conf MiddlewareConfMap) error {
	var mwConf BaseRegexBasicAuthMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, conf); err != nil {
		return fmt.Errorf("error loading %s config: %w", mwConf.Name(), err)
	}
	b.Conf = mwConf

	return nil
}

func (b *BaseRegexBasicAuthMiddleware) Handler(c *gin.Context) {}

func (b *BaseRegexBasicAuthMiddleware) Name() string {
	return "BaseRegexBasicAuthMiddleware"
}

// GetUserFromAuthHeader ensures a valid Basic authorization header and returns the decoded username.
func (b *BaseRegexBasicAuthMiddleware) GetUserFromAuthHeader(authHeader string) (string, error) {
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

// Auth will use the regexes defined in BaseRegexBasicAuthMiddlewareConf to determine
// whether a user is authorized or not.
func (b *BaseRegexBasicAuthMiddleware) AuthUsername(username string) bool {
	return false
}

// RegexBasicAuthAllowMiddleware matches the Basic Authorization token against an
// allow list of regexes. Will set the context `user` key if the passed
// basic auth satisfies the allow/deny list criteria. If auth header is missing,
// the request is denied.
type RegexBasicAuthAllowMiddleware struct {
	*BaseRegexBasicAuthMiddleware
	Conf RegexBasicAuthAllowMiddlewareConf
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

func NewRegexBasicAuthAllowMiddleware() GatewayMiddleware {
	return &RegexBasicAuthAllowMiddleware{}
}

func (b *RegexBasicAuthAllowMiddleware) Name() string {
	return "RegexBasicAuthAllowMiddleware"
}

func (r *RegexBasicAuthAllowMiddleware) Config(conf MiddlewareConfMap) error {
	var mwConf RegexBasicAuthAllowMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, conf); err != nil {
		return fmt.Errorf("error loading %s config: %w", mwConf.Name(), err)
	}

	r.Conf = mwConf

	return nil
}

func (r *RegexBasicAuthAllowMiddleware) Handler(c *gin.Context) {

	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		authUser, err := r.GetUserFromAuthHeader(authHeader)

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

	for _, allowRegex := range r.Conf.Allow {
		allowRe := regexp.MustCompile(allowRegex)

		if allowMatch := allowRe.MatchString(username); allowMatch {
			return true
		}
	}

	return false

}

// RegexBasicAuthDenyMiddleware matches the Basic Authorization token against an
// deny list of regexes. If a match is found, the request is denied.
type RegexBasicAuthDenyMiddleware struct {
	*BaseRegexBasicAuthMiddleware
	Conf RegexBasicAuthDenyMiddlewareConf
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

func NewRegexBasicAuthDenyMiddleware() GatewayMiddleware {
	return &RegexBasicAuthDenyMiddleware{}
}

func (r *RegexBasicAuthDenyMiddleware) Name() string {
	return "RegexBasicAuthDenyMiddleware"
}

func (r *RegexBasicAuthDenyMiddleware) Config(conf MiddlewareConfMap) error {
	var mwConf RegexBasicAuthDenyMiddlewareConf

	if err := LoadMiddlewareConf(&mwConf, conf); err != nil {
		return fmt.Errorf("error loading %s config: %w", mwConf.Name(), err)
	}

	r.Conf = mwConf

	return nil
}

func (r *RegexBasicAuthDenyMiddleware) Handler(c *gin.Context) {

	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		authUser, err := r.GetUserFromAuthHeader(authHeader)
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
	for _, denyRegex := range r.Conf.Deny {
		denyRe := regexp.MustCompile(denyRegex)

		if denyMatch := denyRe.MatchString(username); denyMatch {
			return true
		}
	}

	return false

}
