"""
Single-file reference: Event-driven Airflow / Cloud Composer patterns
Covers:
1) GCS file arrival -> trigger pipeline
2) Pub/Sub driven routing -> BigQuery or Dataflow
3) Efficient processing of 100+ small GCS files
4) SLA monitoring and alerting
5) BigQuery data quality checks
6) Monitoring long-running DAGs and reliability improvements
7) When not to run heavy compute directly in Airflow
8) Where Airflow fits (and does not fit) in real-time streaming

Notes:
- This file is written for readability and interview prep.
- Some imports/operators differ slightly by Composer / provider version.
- Prefer deferrable sensors in Composer 2/3 where available.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator, PubSubPublishMessageOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = Variable.get("project_id", default_var="my-project")
REGION = Variable.get("region", default_var="us-central1")
RAW_BUCKET = Variable.get("raw_bucket", default_var="my-raw-bucket")
PUBSUB_SUBSCRIPTION = Variable.get("router_subscription", default_var="airflow-router-sub")
BQ_DATASET = Variable.get("bq_dataset", default_var="dwh")
BQ_TABLE = Variable.get("bq_table", default_var="events_curated")
ALERT_TOPIC = Variable.get("alert_topic", default_var="composer-alerts")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def log_and_publish_alert(message: str, severity: str = "ERROR"):
    logging.info("AIRFLOW_ALERT severity=%s message=%s", severity, message)


def on_task_failure(context):
    ti = context["task_instance"]
    msg = (
        f"Task failed | dag={ti.dag_id} task={ti.task_id} run_id={ti.run_id} "
        f"try_number={ti.try_number} log_url={ti.log_url}"
    )
    log_and_publish_alert(msg, "ERROR")


def on_dag_failure(context):
    dag_run = context["dag_run"]
    msg = f"DAG failed | dag={dag_run.dag_id} run_id={dag_run.run_id} state={dag_run.state}"
    log_and_publish_alert(msg, "CRITICAL")


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = (
        f"SLA missed | dag={dag.dag_id} tasks={task_list} "
        f"blocking_tasks={blocking_task_list} slas={len(slas)}"
    )
    log_and_publish_alert(msg, "WARNING")


with DAG(
    dag_id="gcs_event_driven_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    on_failure_callback=on_dag_failure,
    sla_miss_callback=on_sla_miss,
    tags=["gcs", "event-driven", "composer"],
) as gcs_event_driven_ingestion:
    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket=RAW_BUCKET,
        object="landing/orders/{{ ds }}/orders.csv",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
        deferrable=True,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=RAW_BUCKET,
        source_objects=["landing/orders/{{ ds }}/orders.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.orders_stg",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    validate_rowcount = BigQueryInsertJobOperator(
        task_id="validate_rowcount",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(COUNT(*) = 0, ERROR('No rows loaded for {{ ds }}'), 'OK')
                FROM `{PROJECT_ID}.{BQ_DATASET}.orders_stg`
                WHERE DATE(_PARTITIONTIME) = DATE('{{{{ ds }}}}')
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
        sla=timedelta(minutes=20),
        on_failure_callback=on_task_failure,
    )

    wait_for_file >> load_to_bq >> validate_rowcount


PREFERRED_EVENT_PATTERN = r"""
Recommended design for truly event-driven GCS ingestion:
1) Configure GCS notification on the bucket to publish OBJECT_FINALIZE events to Pub/Sub.
2) Use Eventarc / Cloud Run / Cloud Function to call Airflow REST API and trigger the DAG.
3) Pass object metadata in dag_run.conf, for example:
   {
     "bucket": "my-raw-bucket",
     "name": "landing/orders/2026-04-30/orders_001.csv",
     "generation": "1714470000000000"
   }
4) DAG reads dag_run.conf and processes only that object.
"""


def _decode_pubsub_messages(pulled_messages) -> List[dict]:
    decoded = []
    for item in pulled_messages or []:
        message = item.get("message", {})
        data = message.get("data", b"")
        if isinstance(data, bytes):
            payload = json.loads(data.decode("utf-8"))
        else:
            payload = json.loads(data)
        decoded.append(payload)
    return decoded


def choose_processing_path(ti, **_):
    pulled = ti.xcom_pull(task_ids="pull_messages")
    decoded = _decode_pubsub_messages(pulled)
    if not decoded:
        return "no_messages"
    payload = decoded[0]
    job_type = payload.get("job_type")
    if job_type == "bigquery":
        return "run_bigquery"
    if job_type == "dataflow":
        return "run_dataflow"
    raise AirflowFailException(f"Unsupported job_type: {job_type}")


with DAG(
    dag_id="pubsub_router_dag",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pubsub", "router"],
    on_failure_callback=on_dag_failure,
) as pubsub_router_dag:
    pull_messages = PubSubPullOperator(
        task_id="pull_messages",
        project_id=PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=10,
        ack_messages=False,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_message",
        python_callable=choose_processing_path,
    )

    no_messages = EmptyOperator(task_id="no_messages")

    run_bigquery = BigQueryInsertJobOperator(
        task_id="run_bigquery",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='pull_messages')[0]['message']['data'].decode('utf-8') | fromjson | get('sql') }}",
                "useLegacySql": False,
            }
        },
        location=REGION,
        on_failure_callback=on_task_failure,
    )

    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        template="{{ ti.xcom_pull(task_ids='pull_messages')[0]['message']['data'].decode('utf-8') | fromjson | get('template') }}",
        parameters="{{ ti.xcom_pull(task_ids='pull_messages')[0]['message']['data'].decode('utf-8') | fromjson | get('parameters') }}",
        on_failure_callback=on_task_failure,
    )

    ack_success = EmptyOperator(
        task_id="ack_success",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    pull_messages >> branch >> [no_messages, run_bigquery, run_dataflow]
    [no_messages, run_bigquery, run_dataflow] >> ack_success


@task
def list_files_for_batch(prefix: str) -> List[str]:
    return [f"{prefix}/file_{i:03d}.json" for i in range(1, 121)]


@task
def chunk_files(file_list: List[str], chunk_size: int = 25) -> List[List[str]]:
    return [file_list[i : i + chunk_size] for i in range(0, len(file_list), chunk_size)]


@task
def build_bq_uris(chunks: List[List[str]]) -> List[List[str]]:
    return [[f"gs://{RAW_BUCKET}/{obj}" for obj in chunk] for chunk in chunks]


with DAG(
    dag_id="gcs_small_files_batched",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["gcs", "batching", "small-files"],
) as gcs_small_files_batched:
    wait_for_prefix = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_prefix",
        bucket=RAW_BUCKET,
        prefix="landing/events/{{ ds_nodash }}/",
        poke_interval=120,
        timeout=60 * 60,
        mode="reschedule",
        deferrable=True,
    )

    files = list_files_for_batch(prefix="landing/events/{{ ds_nodash }}")
    chunks = chunk_files(files, chunk_size=25)
    chunk_uris = build_bq_uris(chunks)

    with TaskGroup(group_id="load_chunks"):
        GCSToBigQueryOperator.partial(
            task_id="load_chunk",
            bucket=RAW_BUCKET,
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.events_stg",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_APPEND",
            autodetect=True,
        ).expand(source_objects=chunks)

    deduplicate = BigQueryInsertJobOperator(
        task_id="deduplicate_and_merge",
        configuration={
            "query": {
                "query": f"""
                MERGE `{PROJECT_ID}.{BQ_DATASET}.events_curated` T
                USING (
                  SELECT * EXCEPT(rn)
                  FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                             PARTITION BY business_key
                             ORDER BY ingest_ts DESC
                           ) AS rn
                    FROM `{PROJECT_ID}.{BQ_DATASET}.events_stg`
                    WHERE DATE(ingest_ts) = DATE('{{{{ ds }}}}')
                  )
                  WHERE rn = 1
                ) S
                ON T.business_key = S.business_key
                WHEN MATCHED THEN UPDATE SET payload = S.payload, ingest_ts = S.ingest_ts
                WHEN NOT MATCHED THEN INSERT ROW
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    wait_for_prefix >> files >> chunks >> chunk_uris >> deduplicate


with DAG(
    dag_id="critical_bq_pipeline_with_sla_and_dq",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_dag_failure,
    sla_miss_callback=on_sla_miss,
    tags=["sla", "dq", "bigquery"],
) as critical_bq_pipeline_with_sla_and_dq:
    extract = EmptyOperator(task_id="extract", sla=timedelta(minutes=15))
    transform = EmptyOperator(task_id="transform", sla=timedelta(minutes=30))

    load_curated = BigQueryInsertJobOperator(
        task_id="load_curated",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.orders_curated` AS
                SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.orders_stg`
                WHERE DATE(ingest_ts) = DATE('{{{{ ds }}}}')
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
        sla=timedelta(minutes=45),
    )

    dq_not_nulls = BigQueryInsertJobOperator(
        task_id="dq_not_nulls",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(COUNT(*) > 0,
                          ERROR('NULL check failed: order_id/customer_id contains NULL'),
                          'OK')
                FROM `{PROJECT_ID}.{BQ_DATASET}.orders_curated`
                WHERE DATE(ingest_ts) = DATE('{{{{ ds }}}}')
                  AND (order_id IS NULL OR customer_id IS NULL)
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
        on_failure_callback=on_task_failure,
    )

    dq_duplicates = BigQueryInsertJobOperator(
        task_id="dq_duplicates",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(COUNT(*) > 0,
                          ERROR('Duplicate check failed: duplicate order_id found'),
                          'OK')
                FROM (
                  SELECT order_id
                  FROM `{PROJECT_ID}.{BQ_DATASET}.orders_curated`
                  WHERE DATE(ingest_ts) = DATE('{{{{ ds }}}}')
                  GROUP BY order_id
                  HAVING COUNT(*) > 1
                )
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
        on_failure_callback=on_task_failure,
    )

    dq_freshness = BigQueryInsertJobOperator(
        task_id="dq_freshness",
        configuration={
            "query": {
                "query": f"""
                SELECT IF(MAX(ingest_ts) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR),
                          ERROR('Freshness check failed'),
                          'OK')
                FROM `{PROJECT_ID}.{BQ_DATASET}.orders_curated`
                WHERE DATE(ingest_ts) = DATE('{{{{ ds }}}}')
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    publish_success_metric = PubSubPublishMessageOperator(
        task_id="publish_success_metric",
        project_id=PROJECT_ID,
        topic=ALERT_TOPIC,
        messages=[{"data": b'{"status":"success","dag":"critical_bq_pipeline_with_sla_and_dq"}'}],
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    extract >> transform >> load_curated >> [dq_not_nulls, dq_duplicates, dq_freshness] >> publish_success_metric


with DAG(
    dag_id="orchestrate_heavy_backfill_external_compute",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["backfill", "external-compute"],
) as orchestrate_heavy_backfill_external_compute:
    start = EmptyOperator(task_id="start")

    launch_dataflow_backfill = DataflowTemplatedJobStartOperator(
        task_id="launch_dataflow_backfill",
        project_id=PROJECT_ID,
        location=REGION,
        template="gs://dataflow-templates/latest/flex/YourFlexTemplate",
        parameters={
            "start_date": "{{ dag_run.conf.get('start_date', '2026-01-01') }}",
            "end_date": "{{ dag_run.conf.get('end_date', '2026-01-31') }}",
            "output_path": f"gs://{RAW_BUCKET}/backfill/output/",
        },
        wait_until_finished=False,
    )

    wait_external = EmptyOperator(task_id="wait_external_completion")
    end = EmptyOperator(task_id="end")

    start >> launch_dataflow_backfill >> wait_external >> end


COMMANDS = r"""
Useful commands
===============
1) Upload DAG to Composer bucket
   gcloud composer environments storage dags import \
     --environment MY_COMPOSER_ENV \
     --location us-central1 \
     --source airflow_composer_patterns.py

2) Trigger a DAG manually
   gcloud composer environments run MY_COMPOSER_ENV \
     --location us-central1 dags trigger -- gcs_event_driven_ingestion

3) Trigger with runtime config
   gcloud composer environments run MY_COMPOSER_ENV \
     --location us-central1 dags trigger -- \
     pubsub_router_dag --conf '{"job_type":"bigquery"}'

4) Create GCS -> Pub/Sub notification
   gcloud pubsub topics create gcs-object-events
   gcloud storage buckets notifications create gs://MY_BUCKET \
     --topic=gcs-object-events \
     --event-types=OBJECT_FINALIZE \
     --payload-format=json

5) Pull sample Pub/Sub messages for testing
   gcloud pubsub subscriptions pull MY_SUBSCRIPTION --limit=5 --auto-ack

6) Tail Composer logs
   gcloud logging read 'resource.type="cloud_composer_environment"' \
     --limit=50 --format='value(textPayload)'

7) List Dataflow jobs
   gcloud dataflow jobs list --region us-central1

8) Query BigQuery duplicates check
   bq query --use_legacy_sql=false 'SELECT order_id, COUNT(*) FROM `my-project.dwh.orders_curated` GROUP BY order_id HAVING COUNT(*) > 1'
"""

if __name__ == "__main__":
    print(PREFERRED_EVENT_PATTERN)
    print(COMMANDS)
