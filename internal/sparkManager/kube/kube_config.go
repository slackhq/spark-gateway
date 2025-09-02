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
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	"github.com/slackhq/spark-gateway/internal/domain"
	"github.com/slackhq/spark-gateway/internal/shared/config"
	"github.com/slackhq/spark-gateway/internal/shared/util"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"

	"strings"
)

func GetKubeConfig(clusterAuthType string, kubeCluster *domain.KubeCluster) (*rest.Config, error) {
	var kubeConfig *rest.Config
	var err error
	klog.Infof("Using cluster auth type: %s", clusterAuthType)
	switch strings.ToLower(clusterAuthType) {
	case config.KubeConfigAuthType:
		kubeConfig, err = NewK8sLocalConfig(kubeCluster)
		if err != nil {
			return nil, fmt.Errorf("unable to generate kube config: %w", err)
		}
	case config.ServiceAccountAuthType:
		caFile := strings.ToLower(kubeCluster.CertificateAuthorityB64File)
		switch caFile {
		case "", "incluster":
			klog.Infof("Using ServiceAccount mounted token and certificate")
			kubeConfig, err = rest.InClusterConfig()
			if err != nil {
				return nil, fmt.Errorf("unable to generate incluster kube config: %w", err)
			}
		case "insecure":
			klog.Warningf("certificateAuthorityB64File is set to 'insecure'. It is recommended to set `cluster.certificateAuthorityB64File` key to a filepath containing the base64 encoded Cluster CA.")

			// Get KubeConfig with EKS token
			kubeConfig, err = GetRestConfig(newEksIamAuthProviderConfig(kubeCluster.Name), kubeCluster.MasterURL, nil)
			if err != nil {
				return nil, fmt.Errorf("unable to generate kube config: %w", err)
			}

		default:
			klog.Infof("Using AuthProvider for generating tokens")
			klog.Infof("Reading %s filepath expecting the base64 encoded Cluster CA", kubeCluster.CertificateAuthorityB64File)
			// Set CAData
			CAb64Bytes, err := dataFromFile(kubeCluster.CertificateAuthorityB64File)
			if err != nil {
				return nil, fmt.Errorf("unable to read certificateAuthorityB64File: %w", err)
			}

			// Get KubeConfig with EKS token
			kubeConfig, err = GetRestConfig(newEksIamAuthProviderConfig(kubeCluster.Name), kubeCluster.MasterURL, util.Ptr(string(CAb64Bytes)))
			if err != nil {
				return nil, fmt.Errorf("unable to generate kube config: %w", err)
			}
		}
	}
	return kubeConfig, nil
}

func dataFromFile(file string) ([]byte, error) {
	if len(file) > 0 {
		fileData, err := os.ReadFile(file)
		if err != nil {
			return []byte{}, err
		}
		return fileData, nil
	}
	return nil, nil
}

func NewK8sLocalConfig(kubeCluster *domain.KubeCluster) (*rest.Config, error) {
	var configPaths []string
	if kubeConfigEnvVar := os.Getenv("KUBECONFIG"); kubeConfigEnvVar != "" {
		// Get Kubeconfig path from KUBECONFIG env var
		configPaths = strings.Split(kubeConfigEnvVar, ":")
	} else {
		// Get Kubeconfig from ~/.kube/config
		userHomeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("error getting user home directory: %w", err)
		}
		configPaths = []string{filepath.Join(userHomeDir, ".kube", "config")}
	}

	loadingRules := clientcmd.ClientConfigLoadingRules{
		Precedence: configPaths,
	}

	kubeConfOverrides := clientcmd.ConfigOverrides{
		CurrentContext: kubeCluster.Name,
	}

	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&loadingRules, &kubeConfOverrides)
	kubeConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating config from KUBECONFIG env var: %w", err)
	}

	klog.Info(kubeConfig.String())
	return kubeConfig, nil

}

func GetRestConfig(authProvider *api.AuthProviderConfig, clusterEndpoint string, eksCAB64 *string) (*rest.Config, error) {

	tlsClientConfig := rest.TLSClientConfig{}

	if eksCAB64 != nil {
		eksCAInfo, err := base64.StdEncoding.DecodeString(*eksCAB64)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64-encoded CA info: %w", err)
		}

		tlsClientConfig.CAData = eksCAInfo
	} else {
		tlsClientConfig.Insecure = true
	}

	klog.Infof("Using EksIamAuthProvider for generating Kube cluster auth tokens")

	return &rest.Config{
		Host:            clusterEndpoint,
		TLSClientConfig: tlsClientConfig,
		AuthProvider:    authProvider,
	}, nil
}
