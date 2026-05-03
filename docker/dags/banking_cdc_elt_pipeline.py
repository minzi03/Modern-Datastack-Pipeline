import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any

import boto3
import snowflake.connector
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "BANKING")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# -------- dbt Config --------
DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

TABLES = ["customers", "accounts", "transactions"]


def validate_config() -> None:
    required = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "MINIO_BUCKET": MINIO_BUCKET,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DB": SNOWFLAKE_DB,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def get_snowflake_connection():
    connect_kwargs = {
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "account": SNOWFLAKE_ACCOUNT,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DB,
        "schema": SNOWFLAKE_SCHEMA,
    }

    if SNOWFLAKE_ROLE:
        connect_kwargs["role"] = SNOWFLAKE_ROLE

    return snowflake.connector.connect(**connect_kwargs)


def list_new_files() -> dict[str, list[str]]:
    validate_config()
    s3 = get_s3_client()
    new_files: dict[str, list[str]] = {}

    for table in TABLES:
        prefix = f"{table}/"
        variable_key = f"processed_minio_keys_{table}"

        try:
            processed_keys = set(
                Variable.get(variable_key, default_var="[]", deserialize_json=True)
            )
        except Exception:
            processed_keys = set()
            logger.warning("Could not read Airflow Variable %s. Using empty set.", variable_key)

        paginator = s3.get_paginator("list_objects_v2")
        table_new_keys: list[str] = []

        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if key.endswith(".parquet") and key not in processed_keys:
                    table_new_keys.append(key)

        table_new_keys.sort()
        new_files[table] = table_new_keys
        logger.info("[%s] Found %s new parquet file(s).", table, len(table_new_keys))

    return new_files


def has_new_files(**context) -> bool:
    files_map = context["ti"].xcom_pull(task_ids="list_new_files")
    has_files = bool(files_map and any(files_map.get(table) for table in TABLES))
    logger.info("Has new files: %s", has_files)
    return has_files


def download_from_minio(**context) -> dict[str, list[dict[str, str]]]:
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = get_s3_client()

    files_map: dict[str, list[str]] = context["ti"].xcom_pull(task_ids="list_new_files")
    downloaded_files: dict[str, list[dict[str, str]]] = {table: [] for table in TABLES}

    for table, keys in files_map.items():
        for key in keys:
            filename = os.path.basename(key)

            with tempfile.NamedTemporaryFile(
                prefix=f"{table}_",
                suffix=f"_{filename}",
                dir=LOCAL_DIR,
                delete=False,
            ) as tmp:
                local_path = tmp.name

            s3.download_file(MINIO_BUCKET, key, local_path)
            logger.info("Downloaded %s -> %s", key, local_path)

            downloaded_files[table].append(
                {
                    "key": key,
                    "local_path": local_path,
                }
            )

    return downloaded_files


def validate_copy_results(table: str, copy_results: list[tuple[Any, ...]]) -> None:
    if not copy_results:
        logger.warning("COPY INTO returned no rows for table %s.", table)
        return

    allowed_statuses = {"LOADED", "PARTIALLY LOADED", "LOAD SKIPPED"}

    for row in copy_results:
        status = str(row[1]).upper() if len(row) > 1 else "UNKNOWN"
        logger.info("COPY result for %s: %s", table, row)

        if status not in allowed_statuses:
            raise RuntimeError(f"COPY INTO failed for table {table}: {row}")


def ensure_table_exists(cur, table: str) -> None:
    sql = f"""
    SELECT COUNT(*)
    FROM {SNOWFLAKE_DB}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = UPPER('{SNOWFLAKE_SCHEMA}')
      AND TABLE_NAME = UPPER('{table}')
    """
    cur.execute(sql)
    count = cur.fetchone()[0]

    if count == 0:
        raise RuntimeError(
            f"Target table {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table.upper()} does not exist."
        )


def load_to_snowflake(**context) -> dict[str, list[str]]:
    downloaded_files: dict[str, list[dict[str, str]]] = context["ti"].xcom_pull(
        task_ids="download_minio"
    )

    if not downloaded_files or all(not downloaded_files[t] for t in TABLES):
        logger.info("No downloaded files found.")
        return {table: [] for table in TABLES}

    conn = get_snowflake_connection()
    cur = conn.cursor()
    loaded_keys: dict[str, list[str]] = {table: [] for table in TABLES}

    try:
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")

        for table, file_entries in downloaded_files.items():
            if not file_entries:
                logger.info("No files for %s, skipping.", table)
                continue

            ensure_table_exists(cur, table)

            for entry in file_entries:
                local_path = entry["local_path"]
                minio_key = entry["key"]

                put_sql = (
                    f"PUT 'file://{local_path}' @%{table} "
                    f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
                cur.execute(put_sql)
                logger.info("Uploaded %s -> @%%%s stage", local_path, table)

                loaded_keys[table].append(minio_key)

            copy_sql = f"""
            COPY INTO {table}
            FROM @%{table}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE
            PURGE = TRUE
            """
            cur.execute(copy_sql)
            copy_results = cur.fetchall()
            validate_copy_results(table, copy_results)

            cur.execute(f"REMOVE @%{table}")
            logger.info("Cleaned Snowflake stage for table: %s", table)

        conn.commit()
        logger.info("Snowflake RAW load completed successfully.")
        return loaded_keys

    except Exception:
        logger.exception("Failed to load files into Snowflake.")
        raise
    finally:
        cur.close()
        conn.close()


def mark_files_processed(**context) -> None:
    loaded_keys: dict[str, list[str]] = context["ti"].xcom_pull(task_ids="load_snowflake")

    if not loaded_keys:
        logger.info("No loaded keys to mark as processed.")
        return

    for table in TABLES:
        variable_key = f"processed_minio_keys_{table}"
        new_loaded = set(loaded_keys.get(table, []))

        try:
            existing = set(
                Variable.get(variable_key, default_var="[]", deserialize_json=True)
            )
        except Exception:
            existing = set()
            logger.warning("Could not read Airflow Variable %s. Using empty set.", variable_key)

        updated = sorted(existing | new_loaded)
        Variable.set(variable_key, updated, serialize_json=True)
        logger.info("[%s] Marked %s file(s) as processed.", table, len(new_loaded))


def cleanup_local_files(**context) -> None:
    downloaded_files: dict[str, list[dict[str, str]]] = context["ti"].xcom_pull(
        task_ids="download_minio"
    )

    if not downloaded_files:
        logger.info("No local files to clean up.")
        return

    for _, file_entries in downloaded_files.items():
        for entry in file_entries:
            local_path = entry["local_path"]
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info("Removed local file: %s", local_path)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="banking_cdc_elt_pipeline",
    default_args=default_args,
    description="End-to-end CDC ELT pipeline from MinIO to Snowflake with dbt snapshots, marts, and tests",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "minio", "snowflake", "dbt", "scd2", "elt"],
) as dag:

    task_list_files = PythonOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    task_has_new_files = ShortCircuitOperator(
        task_id="has_new_files",
        python_callable=has_new_files,
    )

    task_download = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task_load = PythonOperator(
        task_id="load_snowflake_raw",
        python_callable=load_to_snowflake,
    )

    task_mark_processed = PythonOperator(
        task_id="mark_files_processed",
        python_callable=mark_files_processed,
    )

    task_cleanup = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=cleanup_local_files,
        trigger_rule="all_done",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    (
        task_list_files
        >> task_has_new_files
        >> task_download
        >> task_load
        >> task_mark_processed
        >> task_cleanup
        >> dbt_run_staging
        >> dbt_snapshot
        >> dbt_run_marts
        >> dbt_test
    )