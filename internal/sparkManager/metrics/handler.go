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

package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/slackhq/spark-gateway/pkg/config"
)

type Service interface {
	RecordMetrics(done chan bool, ticker *time.Ticker, metrics Metrics)
}

type Handler struct {
	Server  *http.Server
	service Service
}

func NewHandler(service Service, serverConfig config.MetricsServer) *Handler {
	reg := prometheus.NewRegistry()

	reg.MustRegister(Definition.sparkApplicationCount, Definition.cpuAllocated)

	http.Handle(serverConfig.Endpoint, promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	metricsServer := http.Server{
		Addr:    fmt.Sprintf(":%s", serverConfig.Port),
		Handler: nil,
	}
	return &Handler{
		Server:  &metricsServer,
		service: service,
	}
}

func (h *Handler) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	metricsServerDone := make(chan bool)

	go func() {
		h.service.RecordMetrics(metricsServerDone, ticker, Definition)
	}()

	go func() {
		if err := h.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Error(err)
		}
	}()

	<-ctx.Done() // main context is done
	ticker.Stop()
	metricsServerDone <- true
	klog.Info("Metrics Server Exiting.")

}
