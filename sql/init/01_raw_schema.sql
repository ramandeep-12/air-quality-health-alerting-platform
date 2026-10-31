-- Raw layer: source data exactly as received, plus load metadata.
-- Runs automatically the first time the warehouse container starts.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------
-- Hourly air quality readings, one snapshot per pipeline run
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.air_quality_hourly (
    city_id                  TEXT          NOT NULL,
    requested_latitude       NUMERIC(8,5)  NOT NULL,
    requested_longitude      NUMERIC(8,5)  NOT NULL,
    grid_latitude            NUMERIC(8,5)  NOT NULL,
    grid_longitude           NUMERIC(8,5)  NOT NULL,
    grid_elevation_m         NUMERIC(7,1),
    reading_time             TIMESTAMPTZ   NOT NULL,
    pm2_5_ugm3               NUMERIC(10,2),
    pm10_ugm3                NUMERIC(10,2),
    ozone_ugm3               NUMERIC(10,2),
    nitrogen_dioxide_ugm3    NUMERIC(10,2),
    sulphur_dioxide_ugm3     NUMERIC(10,2),
    carbon_monoxide_ugm3     NUMERIC(10,2),
    batch_id                 TEXT          NOT NULL,
    fetched_at               TIMESTAMPTZ   NOT NULL,
    source_object_key        TEXT          NOT NULL,
    loaded_at                TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, batch_id, reading_time)
);

-- dbt will look up the latest version of each city and hour
CREATE INDEX IF NOT EXISTS ix_air_quality_hourly_city_time
    ON raw.air_quality_hourly (city_id, reading_time, fetched_at DESC);

COMMENT ON TABLE raw.air_quality_hourly IS
    'Hourly pollutant readings from the Open-Meteo Air Quality API (CAMS data). Append-only: one snapshot per pipeline run.';
COMMENT ON COLUMN raw.air_quality_hourly.city_id IS 'City identifier from the pipeline cities config.';
COMMENT ON COLUMN raw.air_quality_hourly.grid_latitude IS 'Latitude of the model grid cell the API returned (may differ from the requested point).';
COMMENT ON COLUMN raw.air_quality_hourly.grid_longitude IS 'Longitude of the model grid cell the API returned.';
COMMENT ON COLUMN raw.air_quality_hourly.reading_time IS 'Hour the reading applies to, in UTC.';
COMMENT ON COLUMN raw.air_quality_hourly.batch_id IS 'Airflow run ID that loaded this row. Re-running a batch replaces its rows.';
COMMENT ON COLUMN raw.air_quality_hourly.fetched_at IS 'When the API was called. Used to pick the latest forecast version.';
COMMENT ON COLUMN raw.air_quality_hourly.source_object_key IS 'Object storage path of the raw JSON this row came from.';

-- ---------------------------------------------------------------
-- One row per pipeline run, for monitoring and reconciliation
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.ingestion_runs (
    dataset        TEXT         NOT NULL,
    batch_id       TEXT         NOT NULL,
    city_count     INTEGER      NOT NULL,
    row_count      INTEGER      NOT NULL,
    completed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, batch_id)
);

COMMENT ON TABLE audit.ingestion_runs IS
    'Summary of each ingestion run: how many cities and rows were loaded.';
