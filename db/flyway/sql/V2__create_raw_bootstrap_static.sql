-- raw_staging_temp.bootstrap_static
CREATE TABLE IF NOT EXISTS ${RAW_STAGING_TMP_SCHEMA}.bootstrap_static (
    id BIGSERIAL PRIMARY KEY,         -- unique row id
    data JSONB NOT NULL,              -- full bootstrap-static payload
    fetched_at TIMESTAMPTZ DEFAULT NOW() -- when the payload was fetched
);

-- raw.bootstrap_static
CREATE TABLE IF NOT EXISTS ${RAW_SCHEMA}.bootstrap_static (
    id BIGSERIAL PRIMARY KEY,         -- unique row id
    data JSONB NOT NULL,              -- full bootstrap-static payload
    fetched_at TIMESTAMPTZ DEFAULT NOW() -- when the payload was fetched
);
