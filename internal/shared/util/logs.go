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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type Exception struct {
	Exception_class   string `json:"exception_class"`
	Exception_message string `json:"exception_message"`
	Stacktrace        string `json:"stacktrace"`
}

type LogLine struct {
	Version     int       `json:"@version"`
	Source_host string    `json:"source_host"`
	Msg         string    `json:"msg"`
	Thread_name string    `json:"thread_name"`
	Timestamp   string    `json:"@timestamp"`
	Level       string    `json:"level"`
	Logger_name string    `json:"logger_name"`
	Exception   Exception `json:"exception"`

	UnmarshalledFailedString string
}

func GetLogs(podName string, podNamespace string, tailLines int64, k8sClient *kubernetes.Clientset) (*string, error) {
	podLogOpts := &v1.PodLogOptions{
		TailLines: &tailLines,
	}

	req := k8sClient.CoreV1().Pods(podNamespace).GetLogs(podName, podLogOpts)

	logStream, err := req.Stream(context.Background())
	if err != nil {
		return nil, err
	}

	defer logStream.Close()

	var sb strings.Builder
	if _, err := io.Copy(&sb, logStream); err != nil {
		return nil, err
	}

	str := sb.String()

	return &str, nil
}

func UnmarshalLogLines(logString string) *[]LogLine {
	//logString = strings.ReplaceAll(logString, `\"`, `"`)
	randomString := "sp1N2L3Str4!^"
	// Replace "\n" in "}\n{", "}\n", and "\n{" with a special string which can be used as delimiter for splitting log lines later
	logString = strings.ReplaceAll(logString, "}\n{", fmt.Sprintf("}%s{", randomString))
	// Replace '\n' in '}\n' with special string which can be used as delimiter for splitting log lines later
	logString = strings.ReplaceAll(logString, "}\n", fmt.Sprintf("}%s", randomString))
	// Replace '\n' in '\n{' with special string which can be used as delimiter for splitting log lines later
	logString = strings.ReplaceAll(logString, "\n{", fmt.Sprintf("%s{", randomString))

	// Split by the special string
	logStrings := strings.Split(logString, randomString)

	var logLines []LogLine

	for _, jsonString := range logStrings {
		if jsonString == "" {
			continue
		}

		var logLine LogLine
		err := json.Unmarshal([]byte(jsonString), &logLine)
		if err != nil {
			logLine.UnmarshalledFailedString = jsonString
		}

		logLines = append(logLines, logLine)
	}
	return &logLines
}

func FormatLogLines(logLines *[]LogLine) *string {
	var logStrings string

	for _, logLine := range *logLines {
		var logStr string
		if logLine.UnmarshalledFailedString == "" {
			// If not an exception logLine
			if logLine.Exception.Exception_class == "" && logLine.Exception.Exception_message == "" && logLine.Exception.Stacktrace == "" {
				logStr = fmt.Sprintf("%s %s %s %s %s %s", logLine.Timestamp, logLine.Level, logLine.Source_host, logLine.Thread_name, logLine.Logger_name, logLine.Msg)
			} else {
				logStr = fmt.Sprintf("%s %s %s %s %s %s\nException: %s - %s\n%s", logLine.Timestamp, logLine.Level, logLine.Source_host, logLine.Thread_name, logLine.Logger_name, logLine.Msg, logLine.Exception.Exception_class, logLine.Exception.Exception_message, logLine.Exception.Stacktrace)
			}
		} else {
			logStr = fmt.Sprintf("%s", logLine.UnmarshalledFailedString)
		}
		logStrings = fmt.Sprintf("%s\n%s", logStrings, logStr)
	}

	return &logStrings
}
