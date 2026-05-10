#!/bin/bash
set -e

ENV_NAME="healthcare-airflow"
LOCATION="asia-southeast1"
DAG_ID="parent_dag"

echo "--- Run DAG: $DAG_ID ---"

gcloud composer environments run $ENV_NAME \
    --location $LOCATION \
    dags trigger -- $DAG_ID

echo "--- DAG $DAG_ID is triggered ---"
