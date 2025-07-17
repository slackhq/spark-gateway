{{/*
Expand the name of the chart.
*/}}
{{- define "spark-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Define name of sparkManager components
*/}}
{{- define "spark-gateway.sparkManager.name" -}}
{{- include "spark-gateway.fullname" . }}-sparkmanager
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spark-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "spark-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "spark-gateway.labels" -}}
helm.sh/chart: {{ include "spark-gateway.chart" . }}
{{ include "spark-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spark-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spark-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the database secret to use
*/}}
{{- define "spark-gateway.database.secretName" -}}
{{- if .Values.databaseCredentials.externalSecret.create }}
{{- include "spark-gateway.name" . }}-database
{{- else if ne .Values.databaseCredentials.existingSecret.name "" }}
{{- .Values.databaseCredentials.existingSecret.name }}
{{- else if .Values.postgresql.create }}
{{- include "spark-gateway.fullname" . }}-postgresql
{{- else }}
{{ fail "databaseCredentials.externalSecret.create must be true, or databaseCredentials.existingSecretName must be set, or postgresql.create must be true."}}
{{- end }}
{{- end }}

{{/*
Configure DB_USERNAME and DB_PASSWORD Environment Variables
*/}}
{{- define "spark-gateway.database.passwordEnvVars" -}}
{{- if .Values.databaseCredentials.externalSecret.create }}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-gateway.database.secretName" . }}
      key: {{ .Values.databaseCredentials.externalSecret.passwordProperty }}
{{- else if ne .Values.databaseCredentials.existingSecret.name "" }}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-gateway.database.secretName" . }}
      key: {{ required "databaseCredentials.existingSecret.passwordKey is required if databaseCredentials.existingSecret.name is set!" .Values.databaseCredentials.existingSecret.passwordKey }}
{{- else if .Values.postgresql.create }}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-gateway.database.secretName" . }}
      key: postgres-password
{{- else }}
{{ fail "databaseCredentials.externalSecret.create must be true, or databaseCredentials.existingSecretName must be set, or postgresql.create must be true."}}
{{- end }}
{{- end }}

{{/*
Define the name of configMap with Gateway configs
*/}}
{{- define "spark-gateway.configMapName" -}}
{{- include "spark-gateway.fullname" . }}-config
{{- end -}}

{{/*
Gateway Config Configurations
*/}}
{{- define "spark-gateway.config" -}}
{{- if .Values.postgresql.create }}
{{- $dbHostName := printf "%s-postgresql.%s.svc.cluster.local" (include "spark-gateway.fullname" .) .Release.Namespace }}
{{- $_ := set .Values.config.sparkManager.database "hostname" $dbHostName }}
{{- $_ = set .Values.config.sparkManager.database "port" 5432 }}
{{- $_ = set .Values.config.sparkManager.database "databaseName" "postgres" }}
{{- $_ = set .Values.config.sparkManager.database "username" "postgres" }}
{{- end }}
{{- if or .Values.sparkManager.multiClusterRouting.certificateAuthority.externalSecret.create (ne .Values.sparkManager.multiClusterRouting.certificateAuthority.existingSecretName "") }}
{{- range $_, $cluster := .Values.config.clusters }}
{{- if eq $cluster.certificateAuthorityB64File nil }}
{{- $_ := set $cluster "certificateAuthorityB64File" (printf "%s/%s" $.Values.sparkManager.multiClusterRouting.certificateAuthority.mountPath $cluster.name ) -}}
{{- end }}
{{- end }}
{{- end }}
{{ .Values.config | toYaml | trim }}
{{- end }}
