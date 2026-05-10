#!/bin/bash
set -e

echo "--- Checking DAG syntax ---"

for file in dags/*.py data/ingestion/*.py; do
    echo "Checking: $file"
    python3 -m py_compile "$file"
done

echo "--- Run unit test ---"
pytest tests/test_dag_import.py

echo "--- All tests passed ---"
