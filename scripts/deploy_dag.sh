#!/bin/bash
set -e

BUCKET_NAME="asia-southeast1-healthcare--34176f8a-bucket"

echo "--- Deploy to GCS ---"
gsutil -m rsync -r -d data/ gs://$BUCKET_NAME/data

echo "--- Deploy to Composer ---"
gsutil -m rsync -r -d dags/ gs://$BUCKET_NAME/dags

echo "--- Deploy successfully ---"
