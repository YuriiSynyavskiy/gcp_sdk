import os
from datetime import datetime

import airflow
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from dotenv import load_dotenv

from dist.passcard_scd_dag_callbacks import generate_tmp_table_name, \
    if_new_files, landing_success, landing_to_staging, log_new_files, \
    staging_to_target

load_dotenv()

BUCKET = 'passage'
BUCKET_ARCHIVE = 'passage-archive'
STORAGE_FOLDER = 'passcards'

with airflow.DAG(
    'passcard_scd_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['passcard'],
) as dag:
    get_new_files_from_storage = GCSListObjectsOperator(
        task_id='get_new_files_from_storage',
        bucket=BUCKET,
        prefix='/'.join([STORAGE_FOLDER, 'passcards_']),
        delimiter='.csv',
        on_success_callback=log_new_files,
    )

    if_new_files = ShortCircuitOperator(
        task_id='if_new_files',
        python_callable=if_new_files,
    )

    generate_landing_table_name = PythonOperator(
        task_id='generate_landing_table_name',
        python_callable=generate_tmp_table_name,
    )

    transfer_data_from_storage_to_bq = GCSToBigQueryOperator(
        task_id='transfer_data_from_storage_to_bq',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'person_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'security_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'start_date', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'expires_at', 'type': 'INTEGER'},
        ],
        bucket=BUCKET,
        source_objects='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_project_dataset_table='.'.join([
            os.environ.get('DATASET_LANDING_ID'),
            '{{ ti.xcom_pull(key="tmp_dm_passcard") }}',
        ]),
        skip_leading_rows=1,
        on_success_callback=landing_success,
        external_table=True,
    )

    add_hash_to_records = PythonOperator(
        task_id='add_hash_to_records',
        python_callable=landing_to_staging,
        op_kwargs={
            'landing_table_name': '{{ ti.xcom_pull(key="tmp_dm_passcard") }}',
        },
    )

    archive_processed_files = GCSToGCSOperator(
        task_id='archive_processed_files',
        source_bucket=BUCKET,
        source_object='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_bucket=BUCKET_ARCHIVE,
        destination_object='/'.join([STORAGE_FOLDER, '']),
        move_object=True,
    )

    drop_landing_tmp_table = BigQueryDeleteTableOperator(
        task_id='drop_landing_tmp_table',
        deletion_dataset_table='.'.join([
            os.environ.get('PROJECT_ID'),
            os.environ.get('DATASET_LANDING_ID'),
            '{{ ti.xcom_pull(key="tmp_dm_passcard") }}',
        ]),
    )

    enrich_data_and_put_on_dwh = PythonOperator(
        task_id='enrich_data_and_put_on_dwh',
        python_callable=staging_to_target,
    )

    finish = DummyOperator(task_id='finish')

    get_new_files_from_storage >> if_new_files >> finish

    if_new_files >> generate_landing_table_name >> \
        transfer_data_from_storage_to_bq >> add_hash_to_records >> \
        archive_processed_files >> drop_landing_tmp_table >> \
        enrich_data_and_put_on_dwh >> finish
