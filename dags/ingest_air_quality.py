"""
### Ingest air quality

Daily pipeline that pulls hourly pollutant readings for each active city in
`config/cities.yaml`, keeps the untouched API response in S3 (raw zone), and
merges it into the Iceberg table `aq_raw.air_quality_hourly` through Athena.

**Flow:** get_cities -> fetch_city (mapped per city, saves raw JSON to S3)
-> load_city (mapped per city, MERGE into Iceberg, one at a time)
-> record_run (audit row + updates the `raw_air_quality` asset)

**Idempotent:** re-running a batch overwrites its raw files, and MERGE updates
its rows in place, so nothing is ever duplicated.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
import yaml
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, get_current_context, task

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
AWS_CONN_ID = "aws_aq"
LAKE_BUCKET = "aq-health-239939506885-lake"
ATHENA_WORKGROUP = "aq-health-wg"
TARGET_TABLE = "aq_raw.air_quality_hourly"
AUDIT_TABLE = "aq_audit.ingestion_runs"
CITIES_FILE = Path(__file__).parent / "config" / "cities.yaml"
LOCAL_TZ = "America/Toronto"

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PAST_DAYS = 2
FORECAST_DAYS = 5

# API variable -> table column
POLLUTANT_COLUMNS = {
    "pm2_5": "pm2_5_ugm3",
    "pm10": "pm10_ugm3",
    "ozone": "ozone_ugm3",
    "nitrogen_dioxide": "nitrogen_dioxide_ugm3",
    "sulphur_dioxide": "sulphur_dioxide_ugm3",
    "carbon_monoxide": "carbon_monoxide_ugm3",
}

SOURCE_COLUMNS = [
    "city_id",
    "requested_latitude",
    "requested_longitude",
    "grid_latitude",
    "grid_longitude",
    "grid_elevation_m",
    "reading_time",
    *POLLUTANT_COLUMNS.values(),
    "batch_id",
    "fetched_at",
    "source_object_key",
]
KEY_COLUMNS = ["city_id", "batch_id", "reading_time"]

# Athena's current_timestamp includes a timezone; the table stores UTC timestamp(6)
LOADED_AT_SQL = "CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))"

# Downstream DAGs (dbt in Phase 3) are scheduled on this asset
RAW_AIR_QUALITY = Asset(
    name="raw_air_quality",
    uri=f"s3://{LAKE_BUCKET}/warehouse/aq_raw/air_quality_hourly/",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run_athena(sql: str) -> str:
    """Run one SQL statement in the project workgroup and wait for it to finish."""
    athena = AthenaHook(aws_conn_id=AWS_CONN_ID)
    query_id = athena.run_query(
        query=sql,
        query_context={"Database": "aq_raw"},
        result_configuration={},  # the workgroup decides where results go
        workgroup=ATHENA_WORKGROUP,
    )
    state = athena.poll_query_status(query_id)
    if state != "SUCCEEDED":
        reason = athena.get_state_change_reason(query_id)
        raise RuntimeError(f"Athena query {query_id} {state}: {reason}")
    return query_id


def sql_string(value) -> str:
    """Quote a value as a SQL string literal, escaping single quotes."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_number(value) -> str:
    """Render a number as a SQL literal, rejecting anything that isn't numeric."""
    if value is None:
        return "NULL"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Expected a number, got {value!r}")
    return repr(value)


def sql_timestamp(value: str) -> str:
    """Convert an ISO time (UTC) into an Athena timestamp(6) literal."""
    ts = pendulum.parse(value, tz="UTC").in_timezone("UTC")
    return f"TIMESTAMP '{ts.format('YYYY-MM-DD HH:mm:ss.SSSSSS')}'"


def build_merge_sql(rows: list[str]) -> str:
    """MERGE rows into the raw table: update matching city/batch/hour, insert the rest."""
    pollutant_casts = ",\n        ".join(
        f"CAST({col} AS decimal(10,2)) AS {col}" for col in POLLUTANT_COLUMNS.values()
    )
    update_columns = [col for col in SOURCE_COLUMNS if col not in KEY_COLUMNS]
    update_set = ",\n    ".join(
        [f"{col} = s.{col}" for col in update_columns] + [f"loaded_at = {LOADED_AT_SQL}"]
    )
    insert_columns = ", ".join([*SOURCE_COLUMNS, "loaded_at"])
    insert_values = ", ".join([*(f"s.{col}" for col in SOURCE_COLUMNS), LOADED_AT_SQL])
    rows_sql = ",\n        ".join(rows)

    return f"""
MERGE INTO {TARGET_TABLE} AS t
USING (
    SELECT
        city_id,
        CAST(requested_latitude AS decimal(8,5))  AS requested_latitude,
        CAST(requested_longitude AS decimal(8,5)) AS requested_longitude,
        CAST(grid_latitude AS decimal(8,5))       AS grid_latitude,
        CAST(grid_longitude AS decimal(8,5))      AS grid_longitude,
        CAST(grid_elevation_m AS decimal(7,1))    AS grid_elevation_m,
        reading_time,
        {pollutant_casts},
        batch_id,
        fetched_at,
        source_object_key
    FROM (VALUES
        {rows_sql}
    ) AS v({", ".join(SOURCE_COLUMNS)})
) AS s
ON  t.city_id = s.city_id
AND t.batch_id = s.batch_id
AND t.reading_time = s.reading_time
WHEN MATCHED THEN UPDATE SET
    {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_columns})
    VALUES ({insert_values})
"""


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
@task
def get_cities() -> list[dict]:
    """Read active cities from config, so adding a city needs no code change."""
    config = yaml.safe_load(CITIES_FILE.read_text())
    cities = [city for city in config["cities"] if city.get("active", True)]

    for city in cities:
        if not re.fullmatch(r"[a-z0-9_]+", city["id"]):
            raise ValueError(f"Invalid city id {city['id']!r}: use lowercase letters, digits, underscores")
    if not cities:
        raise ValueError(f"No active cities in {CITIES_FILE}")

    log.info("Ingesting %d cities: %s", len(cities), [city["id"] for city in cities])
    return cities


@task(
    retries=3,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
    execution_timeout=timedelta(minutes=5),
)
def fetch_city(city: dict) -> str:
    """Call the API for one city and store the untouched response in the raw zone."""
    context = get_current_context()
    batch_id = context["run_id"]
    logical_date = context.get("logical_date")
    run_at = pendulum.instance(logical_date) if logical_date else pendulum.now("UTC")
    ingest_date = run_at.in_timezone(LOCAL_TZ).to_date_string()

    response = requests.get(
        API_URL,
        params={
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "hourly": ",".join(POLLUTANT_COLUMNS),
            "past_days": PAST_DAYS,
            "forecast_days": FORECAST_DAYS,
            # No timezone parameter: times come back in UTC
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    # Fail fast if the API contract changed, rather than loading partial data
    hourly = payload.get("hourly", {})
    missing = [field for field in ["time", *POLLUTANT_COLUMNS] if field not in hourly]
    if missing:
        raise ValueError(f"{city['id']}: response is missing hourly fields {missing}")

    payload["_ingestion"] = {
        "city_id": city["id"],
        "requested_latitude": city["latitude"],
        "requested_longitude": city["longitude"],
        "batch_id": batch_id,
        "fetched_at": pendulum.now("UTC").to_iso8601_string(),
        "source_url": API_URL,
    }

    safe_batch = re.sub(r"[^A-Za-z0-9_-]", "_", batch_id)
    object_key = f"raw/air_quality/ingest_date={ingest_date}/city={city['id']}/{safe_batch}.json"
    S3Hook(aws_conn_id=AWS_CONN_ID).load_string(
        json.dumps(payload),
        key=object_key,
        bucket_name=LAKE_BUCKET,
        replace=True,  # a retry of the same batch overwrites its own file
    )
    log.info("Saved %d hourly records to s3://%s/%s", len(hourly["time"]), LAKE_BUCKET, object_key)
    return object_key


@task(max_active_tis_per_dagrun=1, retries=2, retry_delay=timedelta(minutes=1))
def load_city(object_key: str) -> int:
    """MERGE one raw file into the Iceberg table. Runs one at a time to avoid Iceberg commit conflicts."""
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    payload = json.loads(s3.read_key(key=object_key, bucket_name=LAKE_BUCKET))
    meta, hourly = payload["_ingestion"], payload["hourly"]

    rows = []
    for i, reading_time in enumerate(hourly["time"]):
        values = [
            sql_string(meta["city_id"]),
            sql_number(meta["requested_latitude"]),
            sql_number(meta["requested_longitude"]),
            sql_number(payload["latitude"]),
            sql_number(payload["longitude"]),
            sql_number(payload.get("elevation")),
            sql_timestamp(reading_time),
            *(sql_number(hourly[field][i]) for field in POLLUTANT_COLUMNS),
            sql_string(meta["batch_id"]),
            sql_timestamp(meta["fetched_at"]),
            sql_string(object_key),
        ]
        rows.append("(" + ", ".join(values) + ")")

    run_athena(build_merge_sql(rows))
    log.info("Merged %d rows for %s", len(rows), meta["city_id"])
    return len(rows)


@task(outlets=[RAW_AIR_QUALITY])
def record_run(row_counts: list[int]) -> None:
    """Write an audit row and mark the raw_air_quality asset as updated, with run metadata."""
    context = get_current_context()
    batch_id = context["run_id"]
    counts = list(row_counts)
    city_count, total_rows = len(counts), sum(counts)

    expected_rows = city_count * 24 * (PAST_DAYS + FORECAST_DAYS)
    if total_rows != expected_rows:
        log.warning("Expected %d rows but loaded %d", expected_rows, total_rows)

    run_athena(f"""
MERGE INTO {AUDIT_TABLE} AS t
USING (
    SELECT 'raw_air_quality' AS dataset,
           {sql_string(batch_id)} AS batch_id,
           {city_count} AS city_count,
           {total_rows} AS row_count
) AS s
ON t.dataset = s.dataset AND t.batch_id = s.batch_id
WHEN MATCHED THEN UPDATE SET
    city_count = s.city_count,
    row_count = s.row_count,
    completed_at = {LOADED_AT_SQL}
WHEN NOT MATCHED THEN INSERT (dataset, batch_id, city_count, row_count, completed_at)
    VALUES (s.dataset, s.batch_id, s.city_count, s.row_count, {LOADED_AT_SQL})
""")

    # Attach run details to the asset event, visible in the Airflow UI's Assets view
    context["outlet_events"][RAW_AIR_QUALITY].extra = {
        "batch_id": batch_id,
        "city_count": city_count,
        "row_count": total_rows,
    }
    log.info("Batch %s complete: %d cities, %d rows", batch_id, city_count, total_rows)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="ingest_air_quality",
    schedule="0 7 * * *",  # daily at 7 AM Toronto time
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["air-quality", "ingestion", "aws"],
    doc_md=__doc__,
)
def ingest_air_quality():
    cities = get_cities()
    raw_keys = fetch_city.expand(city=cities)
    row_counts = load_city.expand(object_key=raw_keys)
    record_run(row_counts)


ingest_air_quality()