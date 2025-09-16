-- define database tables

CREATE TABLE spark_applications (
    uid UUID PRIMARY KEY,                   -- Updated by Gateway after submission
    batch_id bigint,                        -- Updated by Gateway after submission
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
);

CREATE INDEX idx_spark_applications_batch_id ON spark_applications (batch_id);