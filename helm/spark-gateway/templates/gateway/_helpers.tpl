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
Define name of gateway components
*/}}
{{- define "spark-gateway.gateway.name" -}}
{{- include "spark-gateway.fullname" . }}-gateway
{{- end -}}

{{/*
Common labels for gateway
*/}}
{{- define "spark-gateway.gateway.labels" -}}
{{ include "spark-gateway.labels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spark-gateway.gateway.selectorLabels" -}}
{{ include "spark-gateway.selectorLabels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Define the name of service account
*/}}
{{- define "spark-gateway.gateway.serviceAccountName" -}}
{{- if .Values.gateway.serviceAccount.create }}
{{- default (include "spark-gateway.gateway.name" .) .Values.gateway.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.gateway.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Define ServiceTokenAuthMiddleware validation/setter
*/}}
{{- define "ServiceTokenAuthMiddlewareValidator" }}
{{- range .Values.config.gateway.middleware }}
{{- if and (eq .type "ServiceTokenAuthMiddleware") (not $.Values.gateway.serviceAuth.enabled) }}
{{ fail "If 'ServiceTokenAuthMiddleware' is configured in .Values.config.gateway.middleware, .Values.gateway.serviceAuth must be enabled and configured"}}
{{- end }}
{{- end }}
{{- end }}

{{/*
Define the name of service auth external-secret
*/}}
{{- define "spark-gateway.gateway.ServiceAuthSecretName" -}}
{{- if .Values.gateway.serviceAuth.externalSecret.create }}
{{- include "spark-gateway.gateway.name" . }}-service-auth
{{- else if ne .Values.gateway.serviceAuth.existingSecretName "" }}
{{- .Values.gateway.serviceAuth.existingSecretName }}
{{ else if .Values.gateway.serviceAuth.enabled }}
{{- fail "If gateway.serviceAuth.enabled is 'true' then either gateway.serviceAuth.externalSecret.create must be 'true' or gateway.serviceAuth.existingSecretName must be set."}}
{{- end }}
{{- end }}
