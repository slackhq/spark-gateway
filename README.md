# Spark Gateway

Spark Gateway is a load balancer and a routing gateway for submitting [SparkApplication](https://www.kubeflow.org/docs/components/spark-operator/user-guide/using-sparkapplication/) resources to one or more multiple Kubernetes clusters.

Originally inspired by Apple's [Batch Processing Gateway](https://github.com/apple/batch-processing-gateway), Spark
Gateway's implementation is written in Go, directly integrating with the Go based [kubeflow/spark-operator](https://github.com/kubeflow/spark-operator)
project and using native Go Kubernetes client libraries.

# Overview
## ✨ Features
- 🔌 REST API endpoints to manage [`SparkApplication`](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md) resources
- 🌐 Submission to multiple Kubernetes clusters using a single client
- 🚀 Enables zero downtime deployments and upgrades of Spark-on-k8s infrastructure
- 📝 Enables audit logging of SparkApplication submissions
- 📊 Enables fetching Spark logs via Gateway REST endpoint
- ⚡ Gateway uses a Kubernetes Informer to prevent large number of requests to Kube API Server for tracking
  large number of SparkApplications
- 📋 Enables access to Spark Driver logs via Gateway REST endpoint

# High-level Architecture
![Spark-Gateway Architecture Diagram](.images/architecture.png)

- **🚪 Gateway** is responsible for routing SparkApplication to Kubernetes clusters. It runs a REST server that accepts
  requests from clients and routes requests to the appropriate SparkManager instance.
- **⚙️ SparkManager** runs a REST server and a Kubernetes Informer to track SparkApplication resources in a specific 
  Kubernetes cluster via its Kube API Server. There is one SparkManager per Kubernetes cluster where SparkApplication 
  resources can be submitted. Having cluster specific SparkManager service allows the Spark Gateway to support many Kubernetes
  clusters.

# Build Image

## 🔨 Manual Build
```shell
# Replace x.y.z with the actual new semantic version
docker build -t your.docker.registry/spark-gateway:x.y.z .
docker push your.docker.registry/spark-gateway:x.y.z
```

# Deployment

## 🔧 Deploy via Helm
```shell
# Manual Deployments
helm upgrade \
  --install \
  --kube-context k8s-cluster \
  --namespace spark-gateway \
  spark-gateway .
```

# Configuration

Spark Gateway uses a YAML configuration file that can be passed to both `gateway` and `sparkManager` processes via the `--conf` flag. For detailed configuration options and examples, see [Configuration Documentation](./docs/Configuration.md).

## 🛠️ Development

### 🗄️ sqlc
This project uses sqlc to generate Go code that presents type-safe interfaces to sql queries. The application code calls
the sqlc generated methods.

Generate code
```
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

### 🎭 Moq
[matryer/moq](https://github.com/matryer/moq) project is used to generate mock interfaces.

To generate mocks, run `go generate`.

### 🐘 Local Postgres Database
For local testing, Spark-Gateway will need access to a Postgres database.

#### 📋 Steps to deploy a local Postgres on Kubernetes
```bash
# Set a cluster and namespace in your kubecontext
kubectl config use-context <cluster>
kubectl config set-context --current --namespace=<namespace>

# Install Help charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install my-postgres bitnami/postgresql \
	--set image.registry=your.docker.registry \
  	--set image.repository=bitnami/postgresql \
  	--set image.tag=17.3.0-debian-12-r1 \
  	--set global.security.allowInsecureImages=true

# Get DB Password from Kube Secret
export DB_PASSWORD=$(kubectl get secret --namespace spark-gateway my-postgres-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

# Create table, get the latest schema from pkg/database/repository/schema.sql
kubectl exec -it my-postgres-postgresql-0 -- \
	env PGPASSWORD="$DB_PASSWORD" \
	psql 	-U postgres \
			-d postgres \
			-c "CREATE TABLE spark_applications (
				    uid UUID PRIMARY KEY,                   -- Updated by Gateway after submission
				    name TEXT,                              -- Updated by Gateway after submission
				    creation_time TIMESTAMPTZ,              -- Updated by Gateway after submission
				    termination_time TIMESTAMPTZ,           -- Updated by SparkManager Controller
				    username TEXT,                          -- Updated by Gateway after submission
				    namespace TEXT,                         -- Updated by Gateway after submission
				    cluster TEXT,                           -- Updated by Gateway after submission
				    submitted JSONB,                        -- Updated by Gateway after submission
				    updated JSONB,                          -- Updated by SparkManager Controller
				    state TEXT,                             -- Updated by SparkManager Controller
				    status JSONB                            -- Updated by SparkManager Controller
				);"
  
# Run port-forward
kubectl port-forward service/my-postgres-postgresql 5432:5432
```

Final step is updating Spark-Gateway config.yaml:
```yaml
database:
  databaseName: "postgres"
  hostname: "localhost"
  port: 5432
  username: "postgres" # DB_USERNAME env var is used if not present
  password: "DB_PASSWORD_from_kube_secret" # DB_PASSWORD env var is used if not present
```
