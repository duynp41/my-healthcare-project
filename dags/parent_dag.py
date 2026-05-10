import pendulum

from airflow import DAG
from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define default arguments
default_args = {
    "owner": "DuyNP20",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["duysin41@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

# =========================
# DAG
# =========================
with DAG(
    dag_id="parent_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    description="Parent DAG to trigger PySpark and BigQuery DAGs",
    default_args=default_args,
    tags=["parent", "orchestration", "etl/elt"],
) as dag:

    # Task to trigger PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
        reset_dag_run=True,  # tránh trùng run_id khi rerun
    )

    # Task to trigger BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
        reset_dag_run=True,
    )

# Define dependencies
trigger_pyspark_dag >> trigger_bigquery_dag