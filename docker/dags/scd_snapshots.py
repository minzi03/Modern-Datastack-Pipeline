from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots, marts refresh, and tests for SCD Type 2 pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots", "scd2"],
) as dag:

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

    dbt_snapshot >> dbt_run_marts >> dbt_test