from datetime import datetime

import airflow
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from dotenv import dotenv_values

from callbacks import if_no_files, landing_success, \
    landing_to_staging, log_new_files

config = dotenv_values('.env')

BUCKET = 'passage'
BUCKET_ARCHIVE = 'passage-archive'
STORAGE_FOLDER = 'passcards'

uniq = str(datetime.today().replace(microsecond=0).timestamp())[0:-2]
tmp_table_name = '_'.join(['tmp', config.get('PASSCARD_TABLE_NAME'), uniq])

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

    if_no_files = ShortCircuitOperator(
        task_id='if_no_files',
        python_callable=if_no_files,
    )

    transfer_data_from_storage_to_bq = GCSToBigQueryOperator(
        task_id='transfer_data_from_storage_to_bq',
        schema_fields=[
            {'name': 'passcard_key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'person_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'security_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'start_date', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'expires_at', 'type': 'INTEGER'},
        ],
        bucket=BUCKET,
        source_objects='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_project_dataset_table='.'.join([
            config.get('DATASET_LANDING_ID'), tmp_table_name,
        ]),
        skip_leading_rows=1,
        on_success_callback=landing_success,
    )

    add_source_id_to_records = PythonOperator(
        task_id='add_source_id_to_records',
        python_callable=landing_to_staging,
        op_kwargs={'landing_table_name': tmp_table_name},
    )

    archive_processed_files = GCSToGCSOperator(
        task_id='archive_processed_files',
        source_bucket=BUCKET,
        source_object='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_bucket=BUCKET_ARCHIVE,
        destination_object='/'.join([STORAGE_FOLDER, '']),
        move_object=True,
    )

    enrich_data_and_put_on_dwh = DummyOperator(
        task_id='enrich_data_and_put_on_dwh',
    )

    finish = DummyOperator(task_id='finish')

    get_new_files_from_storage >> if_no_files >> finish

    get_new_files_from_storage >> if_no_files >> \
        transfer_data_from_storage_to_bq >> add_source_id_to_records >> \
        archive_processed_files >> enrich_data_and_put_on_dwh >> finish
