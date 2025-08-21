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

package util

import (
	"bytes"
	"fmt"
	"text/template"
	"time"
)

func ValueExists[T comparable](value T, list []T) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func RenderTemplate(templateStr string, obj interface{}) (*string, error) {
	// Parse the string template
	tmpl, err := template.New("tmpl").Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("failed to generate %s template: %w", templateStr, err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", templateStr, err)
	}
	ret := buf.String()
	return &ret, nil
}

// Returns a merged map.
// map2 will overwrite map1
func MergeMaps(map1, map2 map[string]string) map[string]string {

	for key, value := range map2 {
		map1[key] = value
	}

	return map1
}

func SafeString(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func SafeTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func Ptr[T any](in T) *T {
	return &in
}
