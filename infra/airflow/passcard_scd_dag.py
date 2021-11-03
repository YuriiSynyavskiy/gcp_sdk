import os
from datetime import datetime

import airflow
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from dotenv import load_dotenv

from dist.logger import get_logger
from dist.passcard_scd_dag_callbacks import drop_tmp_passcards_tables, \
    generate_tmp_table_name, if_new_files, landing_to_staging, \
    log_landing_success, log_landing_success_cleaning, log_new_files, \
    log_staging_success, log_targeting_success, staging_to_target
from dist.utils import log_info

load_dotenv()

BUCKET = 'passage'
BUCKET_ARCHIVE = 'passage-archive'
STORAGE_FOLDER = 'passcards'

logger = get_logger('passcard_scd_dag')

with airflow.DAG(
    'passcard_scd_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['passcard'],
) as dag:
    log_start = PythonOperator(
        task_id='log_start',
        python_callable=log_info,
        op_args=[
            logger,
            f'Job started at {datetime.now(tz=None)}',
            f'{{{{ run_id }}}}',
        ],
    )

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
        external_table=True,
        on_success_callback=log_landing_success,
    )

    add_hash_to_records = PythonOperator(
        task_id='add_hash_to_records',
        python_callable=landing_to_staging,
        op_kwargs={
            'landing_table_name': '{{ ti.xcom_pull(key="tmp_dm_passcard") }}',
        },
        on_success_callback=log_staging_success,
    )

    enrich_data_and_put_on_dwh = PythonOperator(
        task_id='enrich_data_and_put_on_dwh',
        python_callable=staging_to_target,
        on_success_callback=log_targeting_success,
    )

    drop_tmp_tables = PythonOperator(
        task_id='drop_tmp_tables',
        python_callable=drop_tmp_passcards_tables,
        on_success_callback=log_landing_success_cleaning,
    )

    archive_processed_files = GCSToGCSOperator(
        task_id='archive_processed_files',
        source_bucket=BUCKET,
        source_object='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_bucket=BUCKET_ARCHIVE,
        destination_object='/'.join([STORAGE_FOLDER, '']),
        move_object=True,
    )

    log_finish = PythonOperator(
        task_id='log_finish',
        python_callable=log_info,
        op_args=[
            logger,
            f'Job finished at {datetime.now(tz=None)}',
            f'{{{{ run_id }}}}',
        ],
    )

    log_start >> get_new_files_from_storage >> if_new_files >> log_finish

    if_new_files >> generate_landing_table_name \
        >> transfer_data_from_storage_to_bq >> add_hash_to_records \
        >> enrich_data_and_put_on_dwh >> drop_tmp_tables \
        >> archive_processed_files >> log_finish
