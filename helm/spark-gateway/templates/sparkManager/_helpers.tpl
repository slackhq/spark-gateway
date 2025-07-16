{{/*
Copyright (c) 2025, Salesforce, Inc.
SPDX-License-Identifier: Apache-2

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Common labels for sparkManager
*/}}
{{- define "spark-gateway.sparkManager.labels" -}}
{{ include "spark-gateway.labels" . }}
app.kubernetes.io/component: sparkmanager
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spark-gateway.sparkManager.selectorLabels" -}}
{{ include "spark-gateway.selectorLabels" . }}
app.kubernetes.io/component: sparkmanager
{{- end }}

{{/*
Define the name of service account
*/}}
{{- define "spark-gateway.sparkManager.serviceAccountName" -}}
{{- if .Values.sparkManager.serviceAccount.create }}
{{- default (include "spark-gateway.sparkManager.name" .) .Values.sparkManager.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.sparkManager.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Define the name of cluster role
*/}}
{{- define "spark-gateway.sparkManager.clusterRoleName" -}}
{{ include "spark-gateway.sparkManager.name" . }}
{{- end }}


{{/*
Create the name of the certificate authority secret to use
*/}}
{{- define "spark-gateway.sparkManager.certificateAuthority.secretName" -}}
{{- if .Values.sparkManager.multiClusterRouting.certificateAuthority.externalSecret.create }}
{{- include "spark-gateway.sparkManager.name" . }}-certificate-authority
{{- else }}
{{- .Values.sparkManager.multiClusterRouting.certificateAuthority.existingSecretName }}
{{- end }}
{{- end }}
