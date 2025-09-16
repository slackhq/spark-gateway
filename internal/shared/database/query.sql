-- define application queries

-- name: GetById :one
SELECT * FROM spark_applications WHERE
uid = @uid;

-- name: InsertSparkApplication :one
INSERT INTO spark_applications (
    uid,
    name,
    creation_time,
    username,
    namespace,
    cluster,
    submitted
)
VALUES (
    @uid, @name, @creation_time, @username, @namespace, @cluster, @submitted::jsonb
)
ON CONFLICT (uid)
DO UPDATE SET
    name = EXCLUDED.name,
    creation_time = EXCLUDED.creation_time,
    username = EXCLUDED.username,
    namespace = EXCLUDED.namespace,
    cluster = EXCLUDED.cluster,
    submitted = EXCLUDED.submitted
RETURNING *;

-- name: UpdateSparkApplication :one
INSERT INTO spark_applications (
    uid,
    termination_time,
    updated,
    state,
    status
)
VALUES (
    @uid, @termination_time, @updated::jsonb, @state, @status::jsonb
)
ON CONFLICT (uid)
DO UPDATE SET
    termination_time = EXCLUDED.termination_time,
    updated = EXCLUDED.updated,
    state = EXCLUDED.state,
    status = EXCLUDED.status
RETURNING *;

-- name: InsertLivyApplication :one
INSERT INTO livy_applications (
    uid
) VALUES (
    @uid
)
RETURNING *;

-- name: GetByBatchId :one
SELECT uid FROM livy_applications
WHERE "batch_id" = @batch_id;

-- name: ListFrom :many
SELECT uid FROM livy_applications
WHERE "batch_id" >= @batch_id
ORDER BY batch_id ASC
LIMIT @size;