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

package gatewayerrors

import (
	"errors"
	"net/http"

	errors2 "k8s.io/apimachinery/pkg/api/errors"
)

type GatewayError struct {
	Status int
	Err    error
}

func (e GatewayError) Error() string {
	return e.Err.Error()
}

func (e GatewayError) Unwrap() error {
	return e.Err
}

// New returns a new GatewayError
func New(status int, err error) GatewayError {
	return GatewayError{
		Status: status,
		Err:    err,
	}
}

// NewFrom returns a new GatewayError wrapping the pass err. If err
// is a GatewayError, will use it's Status and Err, otherwise Status
// will be set to 500
func NewFrom(err error) GatewayError {

	var gatewayErr GatewayError
	if errors.As(err, &gatewayErr) {
		return gatewayErr
	}

	return GatewayError{
		Status: http.StatusInternalServerError,
		Err:    err,
	}
}

func NewInternal(err error) GatewayError {
	return GatewayError{
		Status: http.StatusInternalServerError,
		Err:    err,
	}
}

func NewBadRequest(err error) GatewayError {
	return GatewayError{
		Status: http.StatusBadRequest,
		Err:    err,
	}
}

func NewNotFound(err error) GatewayError {
	return GatewayError{
		Status: http.StatusNotFound,
		Err:    err,
	}
}

func NewAlreadyExists(err error) GatewayError {
	return GatewayError{
		Status: http.StatusConflict,
		Err:    err,
	}
}

func MapK8sErrorToGatewayError(err error) GatewayError {
	switch {
	case errors2.IsAlreadyExists(err):
		return NewAlreadyExists(err)
	case errors2.IsNotFound(err):
		return NewNotFound(err)
	case errors2.IsBadRequest(err):
		return NewBadRequest(err)
	default:
		return NewInternal(err)
	}
}
