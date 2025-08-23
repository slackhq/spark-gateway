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

package kube

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/aws-iam-authenticator/pkg/token"
)

const (
	eksIamAuthPluginName = "eks-iam-auth-plugin"

	cfgClusterName = "cluster-name"
)

func init() {
	if err := rest.RegisterAuthProviderPlugin(eksIamAuthPluginName, newEksIamAuthProvider); err != nil {
		panic(fmt.Sprintf("failed to register %s auth plugin: %v", eksIamAuthPluginName, err))
	}
}

type eksIamAuthProvider struct {
	// Mutex guards persisting to the kubeconfig file and allows synchronized
	// updates to the in-memory config. It also ensures concurrent calls to
	// the RoundTripper only trigger a single refresh request.
	mu sync.Mutex

	currentToken *token.Token
	logger       *slog.Logger
	persister    rest.AuthProviderConfigPersister
	clusterName  string
}

var _ rest.AuthProvider = (*eksIamAuthProvider)(nil)

func newEksIamAuthProviderConfig(clusterName string) *api.AuthProviderConfig {
	return &api.AuthProviderConfig{
		Name: eksIamAuthPluginName,
		Config: map[string]string{
			cfgClusterName: clusterName,
		},
	}
}

func newEksIamAuthProvider(_ string, cfg map[string]string, persister rest.AuthProviderConfigPersister) (rest.AuthProvider, error) {

	clusterName := cfg[cfgClusterName]
	if clusterName == "" {
		return nil, fmt.Errorf("cluster name must be set in config")
	}

	return &eksIamAuthProvider{
		clusterName: clusterName,
		persister:   persister,
		logger:      slog.Default(),
	}, nil
}

func (e *eksIamAuthProvider) WrapTransport(tripper http.RoundTripper) http.RoundTripper {
	return &roundTripper{
		wrapped:  tripper,
		provider: e,
	}
}

func (e *eksIamAuthProvider) Login() error {
	//TODO implement me
	panic("implement me")
}

type roundTripper struct {
	provider *eksIamAuthProvider
	wrapped  http.RoundTripper
}

var _ net.RoundTripperWrapper = &roundTripper{}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		return r.wrapped.RoundTrip(req)
	}

	accessToken, err := r.provider.getToken(req.Context())
	if err != nil {
		return nil, fmt.Errorf("failed getting access token: %w", err)
	}

	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *req
	// deep copy of the Header, so we don't modify the original
	// request's Header (as per RoundTripper contract).
	r2.Header = make(http.Header)
	for k, s := range req.Header {
		r2.Header[k] = s
	}
	r2.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken.Token))

	return r.wrapped.RoundTrip(r2)
}

func (r *roundTripper) WrappedRoundTripper() http.RoundTripper { return r.wrapped }

func (e *eksIamAuthProvider) getToken(ctx context.Context) (*token.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.currentToken != nil && time.Until(e.currentToken.Expiration).Seconds() > 60 {
		return e.currentToken, nil
	}

	e.logger.DebugContext(ctx, fmt.Sprintf("access token is nil or will expire soon; getting cluster access token for cluster %s", e.clusterName))

	tokenGenerator, err := token.NewGenerator(true, false)
	if err != nil {
		return nil, fmt.Errorf("failed getting token generator: %w", err)
	}

	accessToken, err := tokenGenerator.GetWithOptions(&token.GetTokenOptions{
		ClusterID: e.clusterName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster access token for cluster %s: %w", e.clusterName, err)
	}

	e.logger.DebugContext(ctx, fmt.Sprintf("got new token; it will expire in %f mins", time.Until(accessToken.Expiration).Minutes()))

	e.currentToken = &accessToken

	return e.currentToken, nil
}
