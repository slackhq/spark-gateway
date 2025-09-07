package domain

import (
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewGatewaySparkApplication(t *testing.T) {
	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace:   "test",
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp)

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithLabelsAnnotations(t *testing.T) {
	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
			Annotations: map[string]string{
				"annotation": "1",
			},
			Labels: map[string]string{
				"label": "1",
			},
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Annotations: map[string]string{
				"annotation": "1",
			},
			Labels: map[string]string{
				"label": "1",
			},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp)

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithUser(t *testing.T) {

	userStr := "user"

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels: map[string]string{
				GATEWAY_USER_LABEL: "user",
			},
			Annotations: map[string]string{},
		},
		Spec: v1beta2.SparkApplicationSpec{
			ProxyUser: &userStr,
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp, WithUser(userStr))

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithSelector(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels: map[string]string{
				"key": "value",
			},
			Annotations: map[string]string{},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp, WithSelector(map[string]string{"key": "value"}))

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithIdNoName(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Name:        "id",
			Namespace:   "test",
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp, WithId("id"))

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithIdName(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Name:      "appName",
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Name:      "id",
			Namespace: "test",
			Labels:    map[string]string{},
			Annotations: map[string]string{
				"applicationName": "appName",
			},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp, WithId("id"))

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithCluster(t *testing.T) {

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Namespace: "test",
			Labels: map[string]string{
				GATEWAY_CLUSTER_LABEL: "cluster",
			},
			Annotations: map[string]string{},
		},
	}

	gotApp := NewGatewaySparkApplication(&inApp, WithCluster("cluster"))

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}

func TestNewGatewaySparkApplicationWithAll(t *testing.T) {

	user := "user"

	inApp := v1beta2.SparkApplication{
		TypeMeta: v1.TypeMeta{},
		ObjectMeta: v1.ObjectMeta{
			Namespace: "test",
		},
	}

	expected := GatewaySparkApplication{
		GatewayApplicationMeta: GatewayApplicationMeta{
			Name:      "id",
			Namespace: "test",
			Labels: map[string]string{
				GATEWAY_USER_LABEL:    "user",
				GATEWAY_CLUSTER_LABEL: "cluster",
				"key":                 "value",
			},
			Annotations: map[string]string{},
		},
		Spec: v1beta2.SparkApplicationSpec{
			ProxyUser: &user,
		},
	}

	gotApp := NewGatewaySparkApplication(
		&inApp,
		WithCluster("cluster"),
		WithId("id"),
		WithUser("user"),
		WithSelector(map[string]string{"key": "value"}),
	)

	assert.Equal(t, &expected, gotApp, "applications should be the same")
}
