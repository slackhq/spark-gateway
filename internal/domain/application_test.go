package domain

import (
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBaseNewApplication(t *testing.T) {
	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "BaseTest",
			Namespace: "test",
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels:    map[string]string{},
			Annotations: map[string]string{
				"applicationName": "BaseTest",
			},
		},
	}

	newApp := NewGatewayApplication(&inApp)

	assert.Equal(t, &expected, newApp, "applications should be the same")
}

func TestBaseNewApplicationWithLabelsAnnotations(t *testing.T) {
	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "BaseTest",
			Namespace: "test",
			Annotations: map[string]string{
				"annotation": "1",
			},
			Labels: map[string]string{
				"label": "1",
			},
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Annotations: map[string]string{
				"annotation":      "1",
				"applicationName": "BaseTest",
			},
			Labels: map[string]string{
				"label": "1",
			},
		},
	}

	newApp := NewGatewayApplication(&inApp)

	assert.Equal(t, &expected, newApp, "applications should be the same")
}

func TestBaseNewApplicationWithUser(t *testing.T) {

	userStr := "user"

	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "BaseTest",
			Namespace: "test",
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels: map[string]string{
				GATEWAY_USER_LABEL: "user",
			},
			Annotations: map[string]string{
				"applicationName": "BaseTest",
			},
		},
		Spec: GatewayApplicationSpec{
			SparkApplicationSpec: v1beta2.SparkApplicationSpec{
				ProxyUser: &userStr,
			},
		},
		User: "user",
	}

	newApp := NewGatewayApplication(&inApp, WithUser("user"))

	assert.Equal(t, &expected, newApp, "applications should be the same")
}

func TestBaseNewApplicationWithSelector(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "BaseTest",
			Namespace: "test",
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels: map[string]string{
				"key": "value",
			},
			Annotations: map[string]string{
				"applicationName": "BaseTest",
			},
		},
	}

	newApp := NewGatewayApplication(&inApp, WithSelector(map[string]string{
		"key": "value",
	}))

	assert.Equal(t, &expected, newApp, "applications should be the same")
}

func TestBaseNewApplicationWithIdNoName(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Name:        "id",
			Namespace:   "test",
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		GatewayId: "id",
	}

	newApp := NewGatewayApplication(&inApp, WithId("id"))

	assert.Equal(t, &expected, newApp, "applications should be the same")
}

func TestBaseNewApplicationWithIdName(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "appName",
			Namespace: "test",
		},
	}

	expected := GatewayApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Name:      "id",
			Namespace: "test",
			Labels:    map[string]string{},
			Annotations: map[string]string{
				"applicationName": "appName",
			},
		},
		GatewayId: "id",
	}

	newApp := NewGatewayApplication(&inApp, WithId("id"))

	assert.Equal(t, &expected, newApp, "applications should be the same")
}
