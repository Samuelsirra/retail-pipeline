from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('woolworths_batch_pipeline', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    wait_for_data = GCSObjectExistenceSensor(
        task_id='wait_for_sales',
        bucket='woolworths-data-yourproject',
        object='raw/sales/{{ ds_nodash }}/*.csv',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=300,
        timeout=60*60
    )

    start_dataflow = DataflowTemplatedJobStartOperator(
        task_id='start_dataflow',
        template='gs://woolworths-data-yourproject/templates/sales_flex_template.json',
        parameters={
            'input': 'gs://woolworths-data-yourproject/raw/sales/{{ ds }}/sales_*.csv',
            'output_bq': 'your-gcp-project:woolworths.sales',
            'temp_location': 'gs://woolworths-data-yourproject/temp'
        }
    )

    bq_check = BigQueryCheckOperator(
        task_id='bq_row_count_check',
        sql="SELECT COUNT(1) FROM `your-gcp-project.woolworths.sales` WHERE DATE(ts) = DATE('{{ ds }}')",
        use_legacy_sql=False
    )

    wait_for_data >> start_dataflow >> bq_check
