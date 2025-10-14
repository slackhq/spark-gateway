package domain

import (
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/slackhq/spark-gateway/internal/shared/util"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLivyBatchToV1Beta2Application(t *testing.T) {

	createReq := LivyCreateBatchRequest{
		File:      "testFile",
		ProxyUser: "user",
		ClassName: "className",
		Args: []string{
			"arg1",
		},
		Jars: []string{
			"jar1",
		},
		PyFiles: []string{
			"pyFile1",
		},
		Files: []string{
			"file1",
		},
		DriverMemory:   "1G",
		DriverCores:    1,
		ExecutorMemory: "1G",
		ExecutorCores:  1,
		NumExecutors:   10,
		Archives: []string{
			"archive1",
		},
		Queue: "queue",
		Name:  "name",
		Conf: LivyConf{
			"conf1": "val1",
		},
	}

	expected := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		ObjectMeta: v1.ObjectMeta{
			Name: "name",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                "Java",
			SparkVersion:        DEFAULT_SPARK_VERSION,
			Mode:                DEFAULT_SPARK_MODE,
			ProxyUser:           util.Ptr("user"),
			MainClass:           util.Ptr("className"),
			MainApplicationFile: util.Ptr("testFile"),
			Arguments: []string{
				"arg1",
			},
			SparkConf: map[string]string{
				"conf1": "val1",
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       util.Ptr(int32(1)),
					CoreLimit:   util.Ptr("1"),
					Memory:      util.Ptr("1G"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       util.Ptr(int32(1)),
					CoreLimit:   util.Ptr("1"),
					Memory:      util.Ptr("1G"),
				},
				Instances: util.Ptr(int32(10)),
			},
			Deps: v1beta2.Dependencies{
				Jars: []string{
					"jar1",
				},
				Files: []string{
					"file1",
				},
				PyFiles: []string{
					"pyFile1",
				},
				Archives: []string{
					"archive1",
				},
			},
		},
	}

	assert.Equal(t, expected, *createReq.ToV1Beta2SparkApplication(""), "converted create request should match SparkApplication")

}
func TestLivyBatchToV1Beta2ApplicationNamespace(t *testing.T) {

	createReq := LivyCreateBatchRequest{
		File:      "testFile",
		ProxyUser: "user",
		ClassName: "className",
		Args: []string{
			"arg1",
		},
		Jars: []string{
			"jar1",
		},
		PyFiles: []string{
			"pyFile1",
		},
		Files: []string{
			"file1",
		},
		DriverMemory:   "1G",
		DriverCores:    1,
		ExecutorMemory: "1G",
		ExecutorCores:  1,
		NumExecutors:   10,
		Archives: []string{
			"archive1",
		},
		Queue: "queue",
		Name:  "name",
		Conf: LivyConf{
			"conf1": "val1",
		},
	}

	expected := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{
			Kind:       "SparkApplication",
			APIVersion: "sparkoperator.k8s.io/v1beta2",
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                "Java",
			SparkVersion:        DEFAULT_SPARK_VERSION,
			Mode:                DEFAULT_SPARK_MODE,
			ProxyUser:           util.Ptr("user"),
			MainClass:           util.Ptr("className"),
			MainApplicationFile: util.Ptr("testFile"),
			Arguments: []string{
				"arg1",
			},
			SparkConf: map[string]string{
				"conf1": "val1",
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       util.Ptr(int32(1)),
					CoreLimit:   util.Ptr("1"),
					Memory:      util.Ptr("1G"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:       util.Ptr(int32(1)),
					CoreLimit:   util.Ptr("1"),
					Memory:      util.Ptr("1G"),
				},
				Instances: util.Ptr(int32(10)),
			},
			Deps: v1beta2.Dependencies{
				Jars: []string{
					"jar1",
				},
				Files: []string{
					"file1",
				},
				PyFiles: []string{
					"pyFile1",
				},
				Archives: []string{
					"archive1",
				},
			},
		},
	}

	assert.Equal(t, expected, *createReq.ToV1Beta2SparkApplication("namespace"), "converted create request should match SparkApplication")

}

func TestLivySessionState_String(t *testing.T) {
	tests := []struct {
		state    LivySessionState
		expected string
	}{
		{LivySessionStateNotStarted, "not_started"},
		{LivySessionStateStarting, "starting"},
		{LivySessionStateIdle, "idle"},
		{LivySessionStateBusy, "busy"},
		{LivySessionStateRunning, "running"},
		{LivySessionStateShuttingDown, "shutting_down"},
		{LivySessionStateError, "error"},
		{LivySessionStateDead, "dead"},
		{LivySessionStateKilled, "killed"},
		{LivySessionStateSuccess, "success"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestFromV1Beta2ApplicationState(t *testing.T) {
	tests := []struct {
		appState     v1beta2.ApplicationStateType
		expectedLivy LivySessionState
	}{
		{v1beta2.ApplicationStateNew, LivySessionStateNotStarted},
		{v1beta2.ApplicationStateSubmitted, LivySessionStateStarting},
		{v1beta2.ApplicationStateRunning, LivySessionStateRunning},
		{v1beta2.ApplicationStateCompleted, LivySessionStateSuccess},
		{v1beta2.ApplicationStateFailed, LivySessionStateError},
		{v1beta2.ApplicationStateFailedSubmission, LivySessionStateDead},
		{v1beta2.ApplicationStatePendingRerun, LivySessionStateDead},
		{v1beta2.ApplicationStateInvalidating, LivySessionStateShuttingDown},
		{v1beta2.ApplicationStateSucceeding, LivySessionStateShuttingDown},
		{v1beta2.ApplicationStateFailing, LivySessionStateShuttingDown},
		{v1beta2.ApplicationStateUnknown, LivySessionStateDead},
	}

	for _, tt := range tests {
		t.Run(string(tt.appState), func(t *testing.T) {
			result := FromV1Beta2ApplicationState(tt.appState)
			assert.Equal(t, tt.expectedLivy, result)
		})
	}
}

func TestLivyCreateBatchRequest_PythonFileDetection(t *testing.T) {
	createReq := LivyCreateBatchRequest{
		File:      "test.py",
		ProxyUser: "user",
		Name:      "python-job",
	}

	result := createReq.ToV1Beta2SparkApplication("default")

	assert.Equal(t, v1beta2.SparkApplicationTypePython, result.Spec.Type)
}

func TestLivyCreateBatchRequest_JavaFileDetection(t *testing.T) {
	createReq := LivyCreateBatchRequest{
		File:      "test.jar",
		ProxyUser: "user",
		Name:      "java-job",
	}

	result := createReq.ToV1Beta2SparkApplication("default")

	assert.Equal(t, v1beta2.SparkApplicationTypeJava, result.Spec.Type)
}
