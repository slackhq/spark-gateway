# Livy API Documentation

Spark Gateway provides an Apache Livy-compatible REST API for batch job management. This allows existing Livy clients to work with Spark Gateway with minimal changes.

## API Endpoints

All Livy API endpoints are prefixed with `/api/livy/`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/livy/batches` | List all batch jobs |
| POST | `/api/livy/batches` | Create a new batch job |
| GET | `/api/livy/batches/{batchId}` | Get batch job details |
| GET | `/api/livy/batches/{batchId}/state` | Get batch job state |
| GET | `/api/livy/batches/{batchId}/log` | Get batch job logs |
| DELETE | `/api/livy/batches/{batchId}` | Delete a batch job |

## Key Differences from Apache Livy

While Spark Gateway aims to maintain compatibility with Apache Livy's REST API, there are some important differences:

### 1. Batch ID Format
- **Apache Livy**: Uses sequential integer IDs stored in an in-memory session store
- **Spark Gateway**: Uses sequential integer IDs backed by PostgreSQL

**Advantage**: Spark Gateway's persistent storage allows batch recovery after server restart, unlike Apache Livy which loses session data on restart. This allows Spark Gateway to be run with multiple servers for horizontal scaling.

### 2. Namespace Selection
Spark Gateway supports multi-cluster and multi-namespace deployments. You can specify the target namespace using:
- **Header**: `X-Spark-Gateway-Livy-Namespace: <namespace>`
- If not specified, the default namespace from the configuration is used

### 4. Authentication
- **Apache Livy**: Optional authentication
- **Spark Gateway**: Authentication is configurable via middleware (see [internal/gateway/api/middleware](../internal/gateway/api/middleware/))

### 5. Log Retrieval
- **Apache Livy**: Supports both `from` and `size` parameters for log pagination
- **Spark Gateway**: Only supports `size` parameter for tail-based log retrieval

**Note**: The `from` parameter is ignored as Kubernetes logs use tailing. The `size` parameter specifies the number of lines from the end of the log.

### 6. Configuration Options
Spark Gateway supports Livy's configuration options but converts them to SparkApplication specs internally. Some Livy-specific configurations may not have direct equivalents and vice versa.

### 7. State Mapping
Spark Gateway maps SparkApplication states to Livy batch states:

| SparkApplication State | Livy State |
|------------------------|------------|
| New | not_started |
| Submitted | starting |
| Running | running |
| Completed | finished |
| Failed | error |
| FailedSubmission | dead |
| PendingRerun | dead |
| Invalidating | shutting_down |
| Succeeding | shutting_down |
| Failing | shutting_down |
| Unknown | dead |

### 8. AppInfo Fields

The `appInfo` object in batch responses contains the following fields:

| Field | Description | Compatibility |
|-------|-------------|---------------|
| `driverLogUrl` | URL to view driver logs | Apache Livy standard |
| `sparkUiUrl` | URL to Spark UI | Apache Livy standard |
| `sparkHistoryUrl` | URL to Spark History Server | Spark Gateway specific |
| `GatewayId` | Internal Gateway application ID | Spark Gateway specific |
| `Cluster` | Target Kubernetes cluster name | Spark Gateway specific |

**Note**: `sparkHistoryUrl`, `GatewayId`, and `Cluster` are Spark Gateway-specific fields provided for additional functionality. Apache Livy clients can safely ignore these fields.

### 9. Missing Features
The following Apache Livy features are not currently supported:
- Interactive sessions (`/sessions` endpoints)
- Session statements/code execution

## Request/Response Examples

### Create Batch Request
```json
{
  "file": "local:///opt/spark/examples/jars/spark-examples.jar",
  "className": "org.apache.spark.examples.SparkPi",
  "args": ["1000"],
  "conf": {
    "spark.executor.instances": "2",
    "spark.executor.memory": "1g"
  },
  "proxyUser": "user1"
}
```

### List Batches Response
```json
{
  "from": 0,
  "total": 2,
  "sessions": [
    {
      "id": 123,
      "appId": "spark-pi-app",
      "state": "running",
      "appInfo": {
        "driverLogUrl": "http://logs.example.com/driver",
        "sparkUiUrl": "http://spark-ui.example.com",
        "sparkHistoryUrl": "http://spark-history.example.com",
        "GatewayId": "dflt-dflt-01982d11-c2c1-7c3d-8b2f-944ae7248434",
        "Cluster": "production"
      },
      "log": []
    }
  ]
}
```

### Get Batch Response
```json
{
  "id": 123,
  "appId": "spark-pi-app",
  "state": "finished",
  "appInfo": {
    "driverLogUrl": "http://logs.example.com/driver",
    "sparkUiUrl": "http://spark-ui.example.com",
    "sparkHistoryUrl": "http://spark-history.example.com",
    "GatewayId": "dflt-dflt-01982d11-c2c1-7c3d-8b2f-944ae7248434",
    "Cluster": "production"
  },
  "log": []
}
```

### Get Batch State Response
```json
{
  "id": 123,
  "state": "running"
}
```

### Get Batch Logs Response
```json
{
  "id": 123,
  "from": -1,
  "size": 100,
  "log": [
    "2025-10-20 12:00:00 INFO SparkContext: Running Spark version 3.5.0",
    "2025-10-20 12:00:01 INFO SparkContext: Successfully started SparkContext"
  ]
}
```

**Note**: The `from` field in the response is always `-1` as log retrieval is tail-based. The `size` field indicates the number of lines returned from the end of the log.

## Migration from Apache Livy

If you're migrating from Apache Livy to Spark Gateway, you'll need to:

1. Update your base URL to include the `/api/livy/` prefix
2. Configure authentication middleware based on your security requirements
3. Optionally specify the target namespace using the `X-Spark-Gateway-Livy-Namespace` header
4. Update log retrieval code to only use the `size` parameter (tail-based)
5. Remove any code that uses interactive sessions (not supported)

**Benefits of Migration**:
- Persistent batch tracking across server restarts
- Multi-cluster and multi-namespace support
- Flexible authentication options

## Example Migration

**Before (Apache Livy):**
```bash
curl -X POST http://livy-server:8998/batches \
  -H "Content-Type: application/json" \
  -d '{"file": "...", "className": "..."}'
```

**After (Spark Gateway):**
```bash
curl -X POST http://spark-gateway:8080/api/livy/batches \
  -H "Content-Type: application/json" \
  --user gateway-user:pass \
  -H "X-Spark-Gateway-Livy-Namespace: production" \
  -d '{"file": "...", "className": "..."}'
```

**Logs (Apache Livy):**
```bash
# Get 100 lines starting from line 50
curl http://livy-server:8998/batches/123/log?from=50&size=100
```

**Logs (Spark Gateway):**
```bash
# Get last 100 lines (from parameter is ignored)
curl http://spark-gateway:8080/api/livy/batches/123/log?size=100
```
