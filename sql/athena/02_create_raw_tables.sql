CREATE TABLE aq_raw.air_quality_hourly (
    city_id                 string         COMMENT 'City identifier from the pipeline cities config',
    requested_latitude      decimal(8,5)   COMMENT 'Latitude we requested',
    requested_longitude     decimal(8,5)   COMMENT 'Longitude we requested',
    grid_latitude           decimal(8,5)   COMMENT 'Latitude of the model grid cell the API returned',
    grid_longitude          decimal(8,5)   COMMENT 'Longitude of the model grid cell the API returned',
    grid_elevation_m        decimal(7,1)   COMMENT 'Elevation of the grid cell in metres',
    reading_time            timestamp      COMMENT 'Hour the reading applies to, UTC',
    pm2_5_ugm3              decimal(10,2)  COMMENT 'PM2.5 in ug/m3',
    pm10_ugm3               decimal(10,2)  COMMENT 'PM10 in ug/m3',
    ozone_ugm3              decimal(10,2)  COMMENT 'Ozone in ug/m3',
    nitrogen_dioxide_ugm3   decimal(10,2)  COMMENT 'Nitrogen dioxide in ug/m3',
    sulphur_dioxide_ugm3    decimal(10,2)  COMMENT 'Sulphur dioxide in ug/m3',
    carbon_monoxide_ugm3    decimal(10,2)  COMMENT 'Carbon monoxide in ug/m3',
    batch_id                string         COMMENT 'Airflow run ID that loaded this row; re-running a batch replaces its rows',
    fetched_at              timestamp      COMMENT 'When the API was called, UTC; used to pick the latest forecast version',
    source_object_key       string         COMMENT 'S3 key of the raw JSON this row came from',
    loaded_at               timestamp      COMMENT 'When the row was loaded, UTC'
)
PARTITIONED BY (month(reading_time))
LOCATION 's3://aq-health-239939506885-lake/warehouse/aq_raw/air_quality_hourly/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'zstd'
);



CREATE TABLE aq_audit.ingestion_runs (
    dataset        string     COMMENT 'Dataset name, e.g. raw_air_quality',
    batch_id       string     COMMENT 'Airflow run ID',
    city_count     int        COMMENT 'Number of cities loaded',
    row_count      int        COMMENT 'Number of rows loaded',
    completed_at   timestamp  COMMENT 'When the run finished, UTC'
)
LOCATION 's3://aq-health-239939506885-lake/warehouse/aq_audit/ingestion_runs/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);
