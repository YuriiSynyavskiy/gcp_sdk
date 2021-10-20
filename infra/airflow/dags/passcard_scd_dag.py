from datetime import datetime

import airflow
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from dotenv import dotenv_values

config = dotenv_values('.env')

STORAGE_FOLDER = 'passcards'

with airflow.DAG(
    'passcard_scd_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['passcard']
) as dag:
    # TODO: get new files before!
    run_this_last = DummyOperator(
        task_id='run_this_last',
    )

    transfer_data_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='edu-passage-bucket',
        schema_fields=[
            {'name': 'passcard_key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'person_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'security_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'start_date', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'expires_at', 'type': 'INTEGER'},
        ],
        source_objects='/'.join([STORAGE_FOLDER, '*.csv']),
        destination_project_dataset_table='.'.join([
            config.get('DATASET_LANDING_ID'), 'dm_passcard',
        ]),
    )

    transfer_data_to_bq >> run_this_last
