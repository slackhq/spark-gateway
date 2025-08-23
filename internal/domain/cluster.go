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

package domain

import (
	"fmt"
	"regexp"
)

type KubeNamespace struct {
	Name          string  `koanf:"name"`
	NamespaceId   string  `koanf:"id"`
	RoutingWeight float64 `koanf:"routingWeight"`
}

type KubeCluster struct {
	Name                        string          `koanf:"name"`
	ClusterId                   string          `koanf:"id"`
	MasterURL                   string          `koanf:"masterURL"`
	RoutingWeight               float64         `koanf:"routingWeight"`
	Namespaces                  []KubeNamespace `koanf:"namespaces"`
	CertificateAuthorityB64File string          `koanf:"certificateAuthorityB64File"`
}

func (k *KubeCluster) GetNamespaceById(namespaceId string) (KubeNamespace, error) {
	for _, kubeNamespace := range k.Namespaces {
		if kubeNamespace.NamespaceId == namespaceId {
			return kubeNamespace, nil
		}
	}

	return KubeNamespace{}, fmt.Errorf("could not find configured namespace with id '%s' in cluster '%s'", namespaceId, k.Name)
}

func (k *KubeCluster) GetNamespaceByName(name string) (*KubeNamespace, error) {
	for _, kubeNamespace := range k.Namespaces {
		if kubeNamespace.Name == name {
			return &kubeNamespace, nil
		}
	}

	return nil, fmt.Errorf("could not find configured namespace with name '%s' in cluster '%s'", name, k.Name)
}

func ValidateCluster(cluster KubeCluster) (errMessages []string) {
	if cluster.Name == "" || cluster.MasterURL == "" || cluster.ClusterId == "" || len(cluster.Namespaces) == 0 {
		errMessages = append(errMessages, "config error: All items in the 'clusters' list must have 'name', 'masterURL', 'id' and 'namespaces' keys defined")
	}

	// ClusterId validations
	if len(cluster.ClusterId) > 12 {
		errMessages = append(errMessages, "`clusters[].id` must be less than 13 characters")
	}

	// Is ASCII or "-"
	invalidChars, _ := regexp.Compile(`[^a-z0-9]+`)
	match := invalidChars.MatchString(cluster.ClusterId)

	if match {
		errMessages = append(errMessages, "`clusters[].id` can only contain lowercase alphanumeric characters")
	}

	seenNamespaceIds := map[string]bool{}
	for _, kubeNamespace := range cluster.Namespaces {

		// Check if dupe id exists
		_, ok := seenNamespaceIds[kubeNamespace.NamespaceId]
		if ok {
			errMessages = append(errMessages, fmt.Sprintf("duplicate namespace id found in namespaces configuration: '%s'", kubeNamespace.NamespaceId))
		}

		seenNamespaceIds[kubeNamespace.NamespaceId] = true

		if len(kubeNamespace.NamespaceId) > 12 {
			errMessages = append(errMessages, "namespace `id`s must be less than 13 characters")
		}

		namespaceMatch := invalidChars.MatchString(kubeNamespace.NamespaceId)
		if namespaceMatch {
			errMessages = append(errMessages, "namespace `id`s can only contain lowercase alphanumeric characters")
		}
	}

	return errMessages

}
