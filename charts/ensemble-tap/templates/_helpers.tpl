{{- define "ensemble-tap.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ensemble-tap.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ensemble-tap.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ensemble-tap.labels" -}}
helm.sh/chart: {{ include "ensemble-tap.chart" . }}
app.kubernetes.io/name: {{ include "ensemble-tap.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ensemble-tap.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ensemble-tap.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ensemble-tap.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ensemble-tap.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "ensemble-tap.networkPolicyNATSPorts" -}}
{{- $seen := dict -}}
{{- $ports := list -}}
{{- $natsURL := toString (default "" .Values.config.nats.url) -}}
{{- range $raw := splitList "," $natsURL -}}
  {{- $endpoint := trim $raw -}}
  {{- if ne $endpoint "" -}}
    {{- $port := "" -}}
    {{- $match := regexFind ":[0-9]+([/?#].*)?$" $endpoint -}}
    {{- if ne $match "" -}}
      {{- $port = regexFind "[0-9]+" $match -}}
    {{- else -}}
      {{- $port = "4222" -}}
    {{- end -}}
    {{- if and (ne $port "") (not (hasKey $seen $port)) -}}
      {{- $_ := set $seen $port true -}}
      {{- $ports = append $ports (int $port) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- toYaml $ports -}}
{{- end -}}

{{- define "ensemble-tap.networkPolicyClickHousePorts" -}}
{{- $seen := dict -}}
{{- $ports := list -}}
{{- $clickhouseAddr := toString (default "" .Values.config.clickhouse.addr) -}}
{{- range $raw := splitList "," $clickhouseAddr -}}
  {{- $endpoint := trim $raw -}}
  {{- if ne $endpoint "" -}}
    {{- $port := "" -}}
    {{- $match := regexFind ":[0-9]+$" $endpoint -}}
    {{- if ne $match "" -}}
      {{- $port = regexFind "[0-9]+" $match -}}
    {{- else -}}
      {{- $port = "9000" -}}
    {{- end -}}
    {{- if and (ne $port "") (not (hasKey $seen $port)) -}}
      {{- $_ := set $seen $port true -}}
      {{- $ports = append $ports (int $port) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- toYaml $ports -}}
{{- end -}}

{{- define "ensemble-tap.networkPolicyExtraPorts" -}}
{{- $seen := dict -}}
{{- $ports := list -}}
{{- range $extra := (default (list) .Values.networkPolicy.extraEgressPorts) -}}
  {{- $port := trim (printf "%v" $extra) -}}
  {{- if and (ne $port "") (not (hasKey $seen $port)) -}}
    {{- $_ := set $seen $port true -}}
    {{- $ports = append $ports (int $port) -}}
  {{- end -}}
{{- end -}}
{{- toYaml $ports -}}
{{- end -}}

{{- define "ensemble-tap.networkPolicyConfigPorts" -}}
{{- $seen := dict -}}
{{- $ports := list -}}
{{- range $port := (include "ensemble-tap.networkPolicyNATSPorts" . | fromYamlArray) -}}
  {{- $key := printf "%v" $port -}}
  {{- if not (hasKey $seen $key) -}}
    {{- $_ := set $seen $key true -}}
    {{- $ports = append $ports $port -}}
  {{- end -}}
{{- end -}}
{{- range $port := (include "ensemble-tap.networkPolicyClickHousePorts" . | fromYamlArray) -}}
  {{- $key := printf "%v" $port -}}
  {{- if not (hasKey $seen $key) -}}
    {{- $_ := set $seen $key true -}}
    {{- $ports = append $ports $port -}}
  {{- end -}}
{{- end -}}
{{- range $port := (include "ensemble-tap.networkPolicyExtraPorts" . | fromYamlArray) -}}
  {{- $key := printf "%v" $port -}}
  {{- if not (hasKey $seen $key) -}}
    {{- $_ := set $seen $key true -}}
    {{- $ports = append $ports $port -}}
  {{- end -}}
{{- end -}}
{{- toYaml $ports -}}
{{- end -}}
