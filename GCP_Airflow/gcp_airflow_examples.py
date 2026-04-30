from datetime import datetime, timedelta
import json
import time
import random
import requests

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)

# -----------------------------------------------------------------------------
# HOW TO READ THIS FILE
# -----------------------------------------------------------------------------
# This file contains multiple Cloud Composer / Airflow DAG examples for:
# 1) Retries + alerts only after repeated failures
# 2) Retrying only failed tasks instead of re-running the whole DAG
# 3) Handling flaky external APIs safely
# 4) Incremental GCS -> BigQuery loads into partitioned tables without duplicates
# 5) Triggering a downstream DAG after a specific upstream task succeeds
# 6) Cross-project orchestration with secure impersonation
# 7) Triggering a Dataflow template and waiting for completion
#
# You can keep all these DAGs in one file for interview prep / learning.
# In real projects, you would usually split them into separate files.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# COMMON CONFIG
# -----------------------------------------------------------------------------
REGION = "us-central1"
RAW_PROJECT = "prj-raw"
TRANSFORM_PROJECT = "prj-transform"
ANALYTICS_PROJECT = "prj-analytics"
RAW_BUCKET = "raw-csv-bucket"
BQ_DATASET = "analytics_ds"
LANDING_TABLE = "customer_events_landing"
FINAL_TABLE = "customer_events"
DATAFLOW_TEMPLATE_GCS_PATH = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

ALERT_EMAILS = ["data-team@example.com"]

# Service accounts for cross-project orchestration
COMPOSER_WORKER_SA = "composer-runner@orchestration-project.iam.gserviceaccount.com"
RAW_READER_SA = "raw-reader@prj-raw.iam.gserviceaccount.com"
TRANSFORM_RUNNER_SA = "transform-runner@prj-transform.iam.gserviceaccount.com"
BQ_WRITER_SA = "bq-writer@prj-analytics.iam.gserviceaccount.com"


def task_fail_alert(context):
    """
    Fires only when the task is finally marked failed after retries are exhausted.
    This keeps noise low for transient issues like BigQuery quota errors.
    Attach this to on_failure_callback.
    """
    ti = context["ti"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = ti.run_id
    log_url = ti.log_url
    print(
        f"ALERT: DAG={dag_id}, TASK={task_id}, RUN={run_id} failed after retries. Log: {log_url}"
    )
    # Real implementation choices:
    # - Send email using EmailOperator / SMTP
    # - Publish to Pub/Sub -> Cloud Function / Cloud Run -> Slack / PagerDuty
    # - Call incident webhook


DEFAULT_ARGS = {
    "owner": "gayathri",
    "depends_on_past": False,
    "email": ALERT_EMAILS,
    "email_on_retry": False,
    "email_on_failure": False,  # keep False; use callback after retries exhaust
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "on_failure_callback": task_fail_alert,
}

# -----------------------------------------------------------------------------
# 1) BIGQUERY QUOTA ERROR: RETRIES + ALERTS AFTER MULTIPLE FAILURES
# -----------------------------------------------------------------------------
with DAG(
    dag_id="01_bq_retry_and_alert_pattern",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "bigquery", "retries", "alerts"],
) as dag_bq_retry:

    start = EmptyOperator(task_id="start")

    run_bigquery_job = BigQueryInsertJobOperator(
        task_id="run_bigquery_job",
        project_id=ANALYTICS_PROJECT,
        configuration={
            "query": {
                "query": """
                    INSERT INTO `prj-analytics.analytics_ds.customer_events`
                    SELECT *
                    FROM `prj-analytics.analytics_ds.customer_events_staging`
                    WHERE event_date = CURRENT_DATE()
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
        retries=4,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=45),
    )

    end = EmptyOperator(task_id="end")

    start >> run_bigquery_job >> end

# Notes:
# - Airflow retries the failed task instance, not the whole DAG run.
# - on_failure_callback is called when task ends in FAILED state after retries.
# - This is the cleanest way to notify only after repeated consecutive failures.


# -----------------------------------------------------------------------------
# 2) RETRY ONLY FAILED TASKS, NOT THE ENTIRE DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="02_failed_tasks_only_retry",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "retry", "task-level"],
) as dag_failed_tasks_only:

    extract = PythonOperator(
        task_id="extract",
        python_callable=lambda: print("extract completed"),
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=lambda: (_ for _ in ()).throw(AirflowException("sample task failure")),
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    load = PythonOperator(
        task_id="load",
        python_callable=lambda: print("load completed"),
    )

    extract >> transform >> load

# Commands to understand task-level retry behavior:
# airflow dags test 02_failed_tasks_only_retry 2025-01-01
# airflow tasks test 02_failed_tasks_only_retry transform 2025-01-01
# airflow tasks clear 02_failed_tasks_only_retry --task-regex transform --start-date 2025-01-01 --end-date 2025-01-01
#
# Key point:
# - In Airflow, retries are per task instance by default.
# - Do NOT design a wrapper task that reruns the whole pipeline for one task failure.
# - To rerun only failed work from UI/CLI, clear only failed tasks.


# -----------------------------------------------------------------------------
# 3) FLAKY EXTERNAL API: SAFE RETRIES, TIMEOUTS, JITTER, FALLBACK
# -----------------------------------------------------------------------------
def call_external_api_resilient(**context):
    url = Variable.get("flaky_api_url", default_var="https://example.com/api/resource")
    max_attempts_inside_task = 3
    connect_timeout = 5
    read_timeout = 20

    for attempt in range(1, max_attempts_inside_task + 1):
        try:
            response = requests.get(url, timeout=(connect_timeout, read_timeout))

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "30"))
                sleep_s = min(retry_after, 60) + random.randint(1, 5)
                print(f"Rate limited. Sleeping {sleep_s}s before retry.")
                time.sleep(sleep_s)
                continue

            if 500 <= response.status_code < 600:
                sleep_s = min(2 ** attempt + random.randint(1, 5), 30)
                print(f"Server error {response.status_code}. Sleeping {sleep_s}s before retry.")
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            payload = response.json()

            if not payload:
                raise AirflowException("Empty payload from API")

            print(json.dumps(payload)[:1000])
            return payload

        except requests.Timeout:
            sleep_s = min(2 ** attempt + random.randint(1, 5), 30)
            print(f"Timeout. Sleeping {sleep_s}s before retry.")
            time.sleep(sleep_s)
        except requests.RequestException as exc:
            if attempt == max_attempts_inside_task:
                break
            sleep_s = min(2 ** attempt + random.randint(1, 5), 30)
            print(f"RequestException={exc}. Sleeping {sleep_s}s before retry.")
            time.sleep(sleep_s)

    # Fallback behavior:
    # Option 1: fail fast to let Airflow task-level retry happen
    # Option 2: read last successful cached object from GCS / BigQuery
    # Option 3: skip downstream non-critical branch
    raise AirflowException("External API failed after internal retries")


def use_cached_snapshot():
    print("Using cached snapshot from previous successful run")


with DAG(
    dag_id="03_flaky_api_resilient_pattern",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    default_args={
        **DEFAULT_ARGS,
        "retries": 2,  # outer Airflow retry
        "retry_delay": timedelta(minutes=15),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        # Consider pool='external_api_pool' to cap parallel hits on API.
    },
    max_active_runs=1,
    tags=["composer", "api", "resilience"],
) as dag_flaky_api:

    api_call = PythonOperator(
        task_id="api_call",
        python_callable=call_external_api_resilient,
        execution_timeout=timedelta(minutes=10),
        pool="external_api_pool",  # create this pool in Airflow UI to throttle concurrency
    )

    fallback_to_cache = PythonOperator(
        task_id="fallback_to_cache",
        python_callable=use_cached_snapshot,
        trigger_rule="one_failed",
    )

    api_call >> fallback_to_cache

# Design idea:
# - Use a small number of in-task retries for quick transient errors.
# - Use Airflow retries for bigger recovery windows.
# - Use timeouts to avoid hanging workers.
# - Use pool / max_active_runs to prevent API overload.
# - Respect 429 Retry-After when present.


# -----------------------------------------------------------------------------
# 4) INCREMENTAL GCS -> BIGQUERY PARTITIONED LOADS, AVOID DUPLICATES
# -----------------------------------------------------------------------------
def choose_load_or_skip(**context):
    """
    Example branch decision.
    In real life you can inspect manifest table, file metadata table, or naming pattern.
    Example file name: inbound/customer_events/dt=2026-04-30/customer_events_001.csv
    """
    file_already_loaded = False
    return "load_to_landing" if not file_already_loaded else "skip_duplicate_file"


with DAG(
    dag_id="04_gcs_to_bq_incremental_dedup",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    tags=["composer", "gcs", "bigquery", "incremental"],
) as dag_incremental:

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket=RAW_BUCKET,
        object="inbound/customer_events/dt={{ ds }}/customer_events_{{ ds_nodash }}.csv",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )

    create_landing_table = BigQueryCreateEmptyTableOperator(
        task_id="create_landing_table",
        project_id=ANALYTICS_PROJECT,
        dataset_id=BQ_DATASET,
        table_id=LANDING_TABLE,
        exists_ok=True,
        time_partitioning={"type_": "DAY", "field": "event_date"},
        schema_fields=[
            {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
            {"name": "load_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        location=REGION,
    )

    branch_load = BranchPythonOperator(
        task_id="branch_load",
        python_callable=choose_load_or_skip,
    )

    skip_duplicate_file = EmptyOperator(task_id="skip_duplicate_file")

    load_to_landing = GCSToBigQueryOperator(
        task_id="load_to_landing",
        bucket=RAW_BUCKET,
        source_objects=["inbound/customer_events/dt={{ ds }}/customer_events_{{ ds_nodash }}.csv"],
        destination_project_dataset_table=f"{ANALYTICS_PROJECT}.{BQ_DATASET}.{LANDING_TABLE}${{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
        autodetect=False,
        schema_fields=[
            {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
            {"name": "load_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
    )

    merge_to_final = BigQueryInsertJobOperator(
        task_id="merge_to_final",
        project_id=ANALYTICS_PROJECT,
        location=REGION,
        configuration={
            "query": {
                "query": """
                MERGE `prj-analytics.analytics_ds.customer_events` T
                USING (
                  SELECT * EXCEPT(rn)
                  FROM (
                    SELECT
                      event_id,
                      event_ts,
                      event_date,
                      customer_id,
                      source_file,
                      load_ts,
                      ROW_NUMBER() OVER (
                        PARTITION BY event_id
                        ORDER BY load_ts DESC
                      ) AS rn
                    FROM `prj-analytics.analytics_ds.customer_events_landing`
                    WHERE event_date = DATE('{{ ds }}')
                  )
                  WHERE rn = 1
                ) S
                ON T.event_id = S.event_id
                WHEN MATCHED THEN UPDATE SET
                  event_ts = S.event_ts,
                  event_date = S.event_date,
                  customer_id = S.customer_id,
                  source_file = S.source_file,
                  load_ts = S.load_ts
                WHEN NOT MATCHED THEN
                  INSERT (event_id, event_ts, event_date, customer_id, source_file, load_ts)
                  VALUES (S.event_id, S.event_ts, S.event_date, S.customer_id, S.source_file, S.load_ts)
                """,
                "useLegacySql": False,
            }
        },
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule="none_failed_min_one_success",
    )

    wait_for_file >> create_landing_table >> branch_load
    branch_load >> skip_duplicate_file >> finish
    branch_load >> load_to_landing >> merge_to_final >> finish

# Practical duplicate-avoidance methods:
# - Load each file to a landing/staging table first.
# - MERGE from landing to final using a business key such as event_id.
# - Track processed files in a metadata table (file_name, generation, md5, load_ts).
# - Partition by event_date for efficient incremental processing.
# - Cluster final table by event_id / customer_id if query pattern benefits.


# -----------------------------------------------------------------------------
# 5) TRIGGER DOWNSTREAM DAG ONLY AFTER SPECIFIC UPSTREAM TASK SUCCEEDS
# -----------------------------------------------------------------------------
with DAG(
    dag_id="05_upstream_specific_task",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "dependency", "upstream"],
) as dag_upstream:

    upstream_extract = EmptyOperator(task_id="upstream_extract")
    data_quality_passed = EmptyOperator(task_id="data_quality_passed")
    publish_ready = EmptyOperator(task_id="publish_ready")

    trigger_downstream = TriggerDagRunOperator(
        task_id="trigger_downstream_after_specific_task",
        trigger_dag_id="06_downstream_waits_for_specific_upstream_task",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    upstream_extract >> data_quality_passed >> publish_ready >> trigger_downstream


with DAG(
    dag_id="06_downstream_waits_for_specific_upstream_task",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "dependency", "downstream"],
) as dag_downstream:

    wait_for_specific_task = ExternalTaskSensor(
        task_id="wait_for_specific_task",
        external_dag_id="05_upstream_specific_task",
        external_task_id="publish_ready",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    downstream_job = EmptyOperator(task_id="downstream_job")

    wait_for_specific_task >> downstream_job

# Two common patterns:
# A) Same Composer environment: ExternalTaskSensor or TriggerDagRunOperator
# B) Different Composer environment/project: Cloud Composer operators/sensors
#    for remote DAG trigger and status check.


# -----------------------------------------------------------------------------
# 6) MULTI-PROJECT ORCHESTRATION: READ IN ONE PROJECT, TRANSFORM IN ANOTHER,
#    WRITE TO BIGQUERY IN A THIRD
# -----------------------------------------------------------------------------
with DAG(
    dag_id="07_cross_project_secure_orchestration",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "security", "impersonation", "cross-project"],
) as dag_cross_project:

    read_raw = BigQueryInsertJobOperator(
        task_id="read_raw_project_data",
        project_id=RAW_PROJECT,
        location=REGION,
        impersonation_chain=[COMPOSER_WORKER_SA, RAW_READER_SA],
        configuration={
            "query": {
                "query": "SELECT COUNT(*) AS row_count FROM `prj-raw.raw_ds.source_table`",
                "useLegacySql": False,
            }
        },
    )

    transform_data = BigQueryInsertJobOperator(
        task_id="transform_in_transform_project",
        project_id=TRANSFORM_PROJECT,
        location=REGION,
        impersonation_chain=[COMPOSER_WORKER_SA, TRANSFORM_RUNNER_SA],
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `prj-transform.work_ds.customer_events_clean` AS
                    SELECT * FROM `prj-raw.raw_ds.source_table`
                    WHERE event_date = DATE('{{ ds }}')
                """,
                "useLegacySql": False,
            }
        },
    )

    write_final = BigQueryInsertJobOperator(
        task_id="write_to_analytics_project",
        project_id=ANALYTICS_PROJECT,
        location=REGION,
        impersonation_chain=[COMPOSER_WORKER_SA, BQ_WRITER_SA],
        configuration={
            "query": {
                "query": """
                    INSERT INTO `prj-analytics.analytics_ds.customer_events`
                    SELECT * FROM `prj-transform.work_ds.customer_events_clean`
                """,
                "useLegacySql": False,
            }
        },
    )

    read_raw >> transform_data >> write_final

# Secure design notes:
# - Give Composer worker SA minimum roles only.
# - Use impersonation_chain instead of long-lived service account keys.
# - Grant Service Account Token Creator role between chained identities.
# - Put datasets/buckets in separate projects with least privilege IAM.
# - Use VPC-SC / CMEK / private IP Composer if your org requires stronger controls.
# - Prefer one Airflow connection with Workload Identity / environment identity.


# -----------------------------------------------------------------------------
# 7) TRIGGER DATAFLOW TEMPLATE AND WAIT FOR COMPLETION
# -----------------------------------------------------------------------------
with DAG(
    dag_id="08_dataflow_template_wait_for_completion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["composer", "dataflow"],
) as dag_dataflow:

    start_dataflow = DataflowTemplatedJobStartOperator(
        task_id="start_dataflow_template",
        template=DATAFLOW_TEMPLATE_GCS_PATH,
        project_id=TRANSFORM_PROJECT,
        location=REGION,
        job_name="customer-events-load-{{ ds_nodash }}",
        parameters={
            "javascriptTextTransformGcsPath": "gs://config-bucket/udf/transform.js",
            "JSONPath": "gs://config-bucket/schema/customer_events_schema.json",
            "inputFilePattern": f"gs://{RAW_BUCKET}/inbound/customer_events/dt={{{{ ds }}}}/*.csv",
            "outputTable": f"{ANALYTICS_PROJECT}:{BQ_DATASET}.customer_events_staging",
            "bigQueryLoadingTemporaryDirectory": "gs://temp-bucket/bq-load-temp/",
        },
        environment={
            "tempLocation": "gs://temp-bucket/dataflow-temp/",
            "serviceAccountEmail": TRANSFORM_RUNNER_SA,
        },
        wait_until_finished=True,
        deferrable=True,
        poll_sleep=30,
        append_job_name=True,
    )

    after_dataflow = BigQueryInsertJobOperator(
        task_id="run_post_dataflow_sql",
        project_id=ANALYTICS_PROJECT,
        location=REGION,
        configuration={
            "query": {
                "query": "SELECT 1",
                "useLegacySql": False,
            }
        },
    )

    start_dataflow >> after_dataflow

# Key point:
# - wait_until_finished=True makes Airflow wait for batch completion before next task.
# - deferrable=True reduces worker slot usage while polling.
# - For streaming jobs, expected terminal behavior can be different.


# -----------------------------------------------------------------------------
# COMMANDS TO UNDERSTAND / OPERATE
# -----------------------------------------------------------------------------
# List DAGs:
# airflow dags list
#
# Test one DAG:
# airflow dags test 01_bq_retry_and_alert_pattern 2025-01-01
#
# Test one task locally:
# airflow tasks test 03_flaky_api_resilient_pattern api_call 2025-01-01
#
# Retry only failed task by clearing it:
# airflow tasks clear 02_failed_tasks_only_retry --task-regex transform --start-date 2025-01-01 --end-date 2025-01-01
#
# Trigger downstream DAG manually:
# airflow dags trigger 06_downstream_waits_for_specific_upstream_task
#
# Check task logs from Composer:
# gcloud composer environments run <ENV_NAME> \
#   --location <REGION> tasks logs -- \
#   01_bq_retry_and_alert_pattern run_bigquery_job 2025-01-01
#
# Upload DAG to Composer bucket:
# gcloud composer environments describe <ENV_NAME> --location <REGION>
# gsutil cp output/gcp_airflow_examples.py gs://<COMPOSER_DAG_BUCKET>/dags/
#
# Optional package upgrade example for Composer:
# gcloud composer environments update <ENV_NAME> \
#   --location <REGION> \
#   --update-pypi-package apache-airflow-providers-google>=10.0.0
