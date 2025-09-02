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

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"k8s.io/klog/v2"

	"github.com/slackhq/spark-gateway/internal/shared/gatewayerrors"
)

type HttpError struct {
	Error string `json:"error"`
}

func HttpRequest(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, *[]byte, error) {

	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, gatewayerrors.NewFrom(fmt.Errorf("failed to make %s request to %s: %w", req.Method, req.URL, err))
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, gatewayerrors.NewFrom(fmt.Errorf("failed to read response body: %w", err))
	}

	return resp, &responseBody, nil
}

func CheckJsonResponse(resp *http.Response, respBody *[]byte) error {
	if resp != nil && respBody != nil {
		if (resp.StatusCode != http.StatusOK) && (resp.StatusCode != http.StatusCreated) {
			var errMsg string

			var httpError HttpError
			if err := json.Unmarshal(*respBody, &httpError); err != nil {
				klog.Errorf("could not parse error json, will use raw response body: %s", err)
				errMsg = string(*respBody)
			}

			errMsg = errMsg + "\n" + httpError.Error

			return gatewayerrors.New(resp.StatusCode, errors.New(errMsg))
		}
	} else {
		return errors.New("response is empty")
	}

	return nil
}
