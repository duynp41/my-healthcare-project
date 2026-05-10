# Please do not forget to replace information if nessecary

import pendulum
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# =========================
# CONFIG
# =========================
PROJECT_ID = "duynp20-project"
REGION = "asia-southeast1"
CLUSTER_NAME = "healthcare-cluster"
COMPOSER_BUCKET = "asia-southeast1-healthcare--34176f8a-bucket"

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/ingestion/hospitalA_ToLanding.py"
GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/ingestion/hospitalB_ToLanding.py"
GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/ingestion/LandingToBronze_Claims_CPTCodes.py"

def pyspark_job(main_python_file_uri: str) -> dict:
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": main_python_file_uri},
    }

PYSPARK_JOB_1 = pyspark_job(GCS_JOB_FILE_1)
PYSPARK_JOB_2 = pyspark_job(GCS_JOB_FILE_2)
PYSPARK_JOB_3 = pyspark_job(GCS_JOB_FILE_3)

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
    dag_id="pyspark_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    description="Start Dataproc cluster, run PySpark jobs, stop cluster",
    default_args=default_args,
    tags=["pyspark", "dataproc", "etl/elt"],
) as dag:
    
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_1,
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_2,
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_3,
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,  # vẫn stop dù job fail
    )

# Task dependencies
start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> stop_cluster