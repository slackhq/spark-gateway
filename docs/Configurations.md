# Configurations

Spark Gateway uses a YAML configuration file that can be passed to both `gateway` and `sparkManager` processes via the `--conf` flag.

## Top-level Configuration

### `clusters`
List of Kubernetes clusters where SparkApplications can be submitted. Each cluster must have a unique configuration.

> **Note**: Due to the constraint around SparkApplication name length (see [Design Doc](./docs/Design.md)), and to allow admins
the ability to keep their K8s cluster names arbitrary, we use cluster `id`s to make lookups easier while staying within the
character limit. Thus, an example SparkApplication name will look like `clusterid-namespaceid-<UUID>`.

#### Cluster Configuration
Each cluster in the `clusters` list has the following attributes:

- `name` - The cluster's name as specified in the kubeconfig file
- `id` - A user-defined identifier which must be unique per cluster and contain only lowercase letters and numbers (max 12 characters)
- `masterURL` - The Kubernetes API server hostname
- `routingWeight` - Weight for load balancing (defaults to 1.0 if not specified)
- `namespaces` - List of [namespaces](#namespace-configuration) supported by the cluster.
- `certificateAuthorityB64File` - Path to a file containing the base64 encoded certificate authority (only used if `sparkManager.clusterAuthType` is set to `serviceaccount`)

**Certificate Authority Options (`certificateAuthorityB64File` config):**
- Set to `incluster` or leave unset. This is the default option, Spark Gateway will read the CA from `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`.
  Use this option if Spark Gateway is deployed on the same cluster to which it submits SparkApplications.
- Set to `insecure` to disable certificate validation (unencrypted, insecure traffic to Kube API Server)
- Set to a file path containing the base64 encoded certificate authority (typically mounted to pod via K8s Secret).
  Use this option if Spark Gateway is deployed on a different K8s cluster than where the SparkApplications are submitted.

> Note: It is not recommended to change a cluster's `id` once jobs are deployed to that cluster. If the `id` is updated,
> Gateway will lose track of jobs that were submitted using the older `id`.

#### Namespace Configuration
Each namespace in a cluster has:
- `name` - The Kubernetes namespace name
- `id` - A user-defined identifier (max 12 characters, lowercase alphanumeric only)
- `routingWeight` - Weight for load balancing within the namespace (defaults to 1.0 if not specified)

#### Example
```yaml
clusters:
  - name: dev-k8s-cluster
    id: dev1
    masterURL: your.k8s.api.server
    routingWeight: 10
    certificateAuthorityB64File: /etc/certificate-authority
    namespaces:
      - name: team-a-spark
        id: teama
        routingWeight: 1
```

### `clusterRouter`
Configuration for routing SparkApplications to clusters.

#### Router Types
- `random` - Random selection between available clusters
- `weightBased` - Weight-based selection using Prometheus metrics (WIP)
- `weightBasedRandom` - Weight-based random selection (default)

#### Configuration Options
- `type` - Primary router type
- `fallbackType` - Router type to use if primary fails (recommended: `random` or `weightBasedRandom`)
- `dimension` - Routing dimension: `namespace` or `cluster`. This determines whether `namespace` or `cluster` level
  metrics are used to determine the best cluster to route a new SparkApplication to during the cluster selection process
  at submission time. For instance, if `type` is set to `weightBased`, `dimension` is set to `namespace` and
  `prometheusQuery.metric` is set to `spark_application_count`, then Spark Gateway will use the number of spark
  applications in the namespace specified in the submitted SparkApp to determine the best cluster to route to. However,
  if the dimension is set to `cluster`, then Spark Gateway will use the number of spark applications in the cluster
  including all namespaces in the cluster, to determine the best cluster to route to.
- `prometheusQuery` - Configuration for Prometheus metrics queries

#### Prometheus Query Configuration
```yaml
clusterRouter:
  type: weightBased
  fallbackType: weightBasedRandom
  dimension: namespace  # Equivalent to `spark_application_count{"namespace":"specified-NS"}` PromQL
  prometheusQuery:
    metric: spark_application_count  # Should be a gauge metric
```

### `defaultLogLines`
The default number of lines to return when getting logs from a driver if the `lines` query parameter is not provided with the request.

### `mode` (optional)
Operating mode of the Spark Gateway. Common values include `local` for development.

### `selectorKey` and `selectorValue`
Used to label and filter SparkApplications managed by Spark Gateway:

1. Spark Gateway adds "`selectorKey`=`selectorValue`" labels to all SparkApplications it creates
2. Gateway endpoints only recognize SparkApplications with these labels
3. SparkManager only monitors SparkApplications with these labels, reducing memory footprint

#### Recommended Configuration
```yaml
selectorKey: "spark-gateway/owned"
selectorValue: "true"
```

#### Default (No Filtering)
```yaml
selectorKey: ""
selectorValue: ""
```

### `sparkManagerPort`
Defines the port used by the SparkManager server.

## Gateway Configuration

### `gateway`
Gateway server configuration.

#### `gatewayApiVersion`
Specifies the API version path in the URL. For example, if set to `v2`, the base path for all endpoints would be
`https://hostname/v2/applications/`.

#### `gatewayPort`
Defines the port used by the Gateway server.

#### `middleware`
List of middleware to apply to Gateway requests. Available middleware types:
- `RegexBasicAuthAllowMiddleware` - Allow requests based on regex patterns
- `RegexBasicAuthDenyMiddleware` - Deny requests based on regex patterns
- `HeaderAuthMiddleware` - Authenticate based on HTTP headers
- `ServiceTokenAuthMiddleware` - Authenticate using service tokens

#### Middleware Configuration Examples

**Regex Basic Auth:**
```yaml
middleware:
  - type: RegexBasicAuthAllowMiddleware
    conf:
      allow:
        - .*
```

**Header Auth:**
```yaml
middleware:
  - type: HeaderAuthMiddleware
    conf:
      headers:
        - key: Auth-User
```

**Service Token Auth:**
```yaml
middleware:
  - type: ServiceTokenAuthMiddleware
    conf:
      serviceTokenMapFile: /conf/service-auth-config.yaml
```

#### `statusUrlTemplates`
Templates for generating status URLs. Any field from [`v1beta2.SparkApplication`](https://github.com/kubeflow/spark-operator/blob/920772e065394006529f659513182ea7a8f873d2/docs/api-docs.md#sparkoperator.k8s.io/v1beta2.SparkApplication) can be used for templating.
See [SparkApplication API Docs](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md#sparkoperator.k8s.io/v1beta2.SparkApplication)
and the [SparkApplication struct](https://github.com/kubeflow/spark-operator/blob/3128c7f157d9da00f5b9401a161a9353bcad5cad/api/v1beta2/sparkapplication_types.go#L187)
for reference.

```yaml
statusUrlTemplates:
  sparkUI: "{{.Status.DriverInfo.WebUIIngressAddress}}"
  sparkHistoryUI: "https://spark-history-{{.ObjectMeta.Namespace}}.example.com/history/{{.Status.SparkApplicationID}}/jobs"
  logsUI: "https://kibana.example.com/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-1d,to:now))&_a=(interval:auto,query:(language:lucene,query:'host:%20%22{{.ObjectMeta.Name}}-driver%22'),sort:!(!('@timestamp',desc)))"
```

## SparkManager Configuration

### `sparkManager`
SparkManager server configuration.

#### `clusterAuthType`
Authentication type for accessing Kubernetes clusters:
- `serviceaccount` - Use service account credentials (recommended for production). See all certificate authority options in
  [Cluster Configurations](#cluster-configuration) section.
- `kubeconfig` - Use local kubeconfig (typically for development)

#### `database`
Database configuration for persisting submission requests and SparkApplication specs.

**Connection string format**: `postgres://{username}:{password}@{hostname}:{port}/{databaseName}`

**Configuration Options:**
- `enable` - Enable database functionality
- `databaseName` - Database name
- `hostname` - Database hostname
- `port` - Database port
- `username` - Database username (can use `DB_USERNAME` environment variable)
- `password` - Database password (can use `DB_PASSWORD` environment variable)

**Setting Database Credentials:**
Username and password can be set via configuration or environment variables. If `database.username` and `database.password` are not present, `DB_USERNAME` and `DB_PASSWORD` environment variables will be used.

For security reasons, it's recommended to use environment variables for production deployments.

#### `metricsServer`
Metrics server configuration for Prometheus metrics.

```yaml
metricsServer:
  endpoint: "/metrics"
  port: "9090"
```

## Debug Configuration

### `debugPorts`
Allows developers to set custom ports for different SparkManagers to avoid port collisions during local development.

```yaml
debugPorts:
  cluster-name:
    sparkManagerPort: "8085"
    metricsPort: "9095"
```

See configurations in [configs/local.yaml](configs/local.yaml)
