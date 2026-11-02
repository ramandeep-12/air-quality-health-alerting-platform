from __future__ import annotations

import logging

import pendulum
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

AWS_CONN_ID = "aws_aq"
LAKE_BUCKET = "aq-health-239939506885-lake"
ATHENA_WORKGROUP = "aq-health-wg"


@task
def check_s3_write_read_delete() -> None:
    """Write a small file, read it back, then delete it."""
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    key = "healthcheck/airflow_connection_check.txt"

    s3.load_string("ok", key=key, bucket_name=LAKE_BUCKET, replace=True)
    content = s3.read_key(key=key, bucket_name=LAKE_BUCKET)
    s3.delete_objects(bucket=LAKE_BUCKET, keys=[key])

    log.info("S3 write, read and delete worked (read back: %r)", content)


@task
def check_athena_query() -> None:
    """Query the raw Iceberg table through the project workgroup."""
    athena = AthenaHook(aws_conn_id=AWS_CONN_ID)
    query_id = athena.run_query(
        query="SELECT count(*) FROM aq_raw.air_quality_hourly",
        query_context={"Database": "aq_raw"},
        result_configuration={},  # the workgroup decides where results go
        workgroup=ATHENA_WORKGROUP,
    )

    state = athena.poll_query_status(query_id)
    if state != "SUCCEEDED":
        reason = athena.get_state_change_reason(query_id)
        raise RuntimeError(f"Athena query {state}: {reason}")

    results = athena.get_query_results(query_id)
    row_count = results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
    log.info("Athena query succeeded: aq_raw.air_quality_hourly has %s rows", row_count)


@dag(
    dag_id="aws_connection_check",
    schedule=None,  # run manually only
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Toronto"),
    catchup=False,
    tags=["setup"],
    doc_md=__doc__,
)
def aws_connection_check():
    check_s3_write_read_delete() >> check_athena_query()


aws_connection_check()