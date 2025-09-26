# Architecture: Retail Batch Pipeline

Local/Store Systems -> (daily files) -> GCS (raw) -> Dataflow (batch transforms) -> BigQuery (partitioned & clustered)
                                            ^
                                            |
                                         Airflow (Cloud Composer)

- Raw zone: gs://.../raw/sales/YYYY/MM/DD/
- Processed zone: gs://.../processed/parquet/...
- BigQuery: partition by DATE(ts), cluster by (store_id, product_id)
- Orchestration: Sensor -> Dataflow template -> validation checks -> archive
