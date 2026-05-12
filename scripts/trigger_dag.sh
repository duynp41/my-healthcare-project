#!/bin/bash
set -e

ENV_NAME="healthcare-airflow"
LOCATION="asia-southeast1"

DAG_IDS=("bigquery_dag" "pyspark_dag" "parent_dag")

echo "--- TRIGGER DAGS ---"

for DAG_ID in "${DAG_IDS[@]}"
do
    echo "--------------------------------------------"
    echo "Triggering: $DAG_ID..."
    
    gcloud composer environments run $ENV_NAME \
        --location $LOCATION \
        dags trigger -- "$DAG_ID"
        
    echo "Status: DAG $DAG_ID done."
done

echo "--------------------------------------------"
echo "--- All DAGs are triggered successfully ---"
