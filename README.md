# Woolworths Retail Batch Pipeline (sample)

This repository contains a minimal, runnable example of a **retail batch pipeline** (GCS -> Dataflow -> BigQuery)
with orchestration via Airflow (Cloud Composer).

## What you'll find
- `pipeline/` : Apache Beam pipeline code (batch)
- `dags/` : Airflow DAG to trigger the Dataflow Flex template
- `infra/` : Example Cloud Build config to build+push image and create a flex template
- `sample_data/` : Small CSV sample for quick testing
- `tests/` : Unit test for the parser
- `notebooks/` : Colab / local notebook with step-by-step instructions

## Quick local test (DirectRunner)
1. Create and activate a virtualenv
   ```
   python -m venv venv && source venv/bin/activate
   pip install apache-beam[gcp]
   ```
2. Run the pipeline locally:
   ```
   python pipeline/sales_pipeline.py --input sample_data/sales_2025-09-20.csv --output_bq project:dataset.sales --temp_location /tmp
   ```
3. Run unit tests:
   ```
   pip install pytest
   pytest tests/test_parse.py
   ```

## Deploy to GCP (high level)
- Create GCS buckets, BigQuery dataset/table, service accounts (see earlier instructions).
- Build docker image, push to GCR, create Dataflow Flex template (cloudbuild.yaml shows an example).
- Deploy DAG to Composer, configure connections.

## Notes
This is a starter template — production hardening (secrets, retries, monitoring, IAM hardening, schema evolution handling) is required before using in real environments.
