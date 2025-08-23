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

	"github.com/gin-gonic/gin"
)

// IsAuthed ensures that a User is set in the context. This should be used as the final
// auth in a middleware chain to ensure proper routing.
func IsAuthed(c *gin.Context) {
	if user, ok := c.Get("user"); ok {
		if user == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "No valid user provided"})
		}
		c.Next()
	} else {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "User is unauthorized"})
	}
}
