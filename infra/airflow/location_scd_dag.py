import os
import datetime
import airflow
from dist import tables
from dist.logger import get_logger
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from dist.sql_queries import sql_landing_to_staging_location, sql_staging_to_target_location
from dist.utils import define_file, check_file, check_recursive
from data_generating.location_data import create_data

LOG_NAME = 'location-dag'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'location_scd_dag',
        start_date=datetime.datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
    
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=create_data,
        dag=dag
    )

    check_stream_state_task = PythonOperator(
        task_id='check_stream_state_task',
        python_callable=define_file,
        op_kwargs={'path': 'locations/'},
        dag=dag,
    )

    find_file_to_process_task = BranchPythonOperator(
        task_id='find_file_to_process_task',
        python_callable=check_file,
        op_kwargs={'namespace': 'location', 'LOG_NAME': LOG_NAME},
        dag=dag,
    )

    nothing_to_process_task = DummyOperator(
        task_id='nothing_to_process',
        dag=dag
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=Variable.get("BUCKET_ID"),
        source_objects=[
            "{{ ti.xcom_pull(task_ids='check_stream_state_task')}}"],
        destination_project_dataset_table=f"{Variable.get('LANDING_DATASET_ID')}.{tables.location_table_name}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'run_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location_key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'building_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'security_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'gate_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'room_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'floor', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        dag=dag,
    )
    
    landing_to_staging = BigQueryOperator(
        dag=dag,
        task_id='landing_to_staging',
        location='US',
        use_legacy_sql=False,
        retries=0,
        write_disposition='WRITE_TRUNCATE',
        sql=sql_landing_to_staging_location
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=sql_staging_to_target_location
    )

    archive_file = GCSToGCSOperator(
        task_id='archive_file',
        source_bucket=Variable.get("BUCKET_ID"),
        source_objects=["{{ti.xcom_pull(task_ids='check_stream_state_task')}}"],
        destination_bucket=Variable.get("BUCKET_ID"),
        destination_object="processed_{{ti.xcom_pull(task_ids='check_stream_state_task')}}",
        move_object=True
    )

    check_recursive_task = BranchPythonOperator(
        task_id='check_recursive_task',
        python_callable=check_recursive,
        op_kwargs={'destination_object': "processed_{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'source_object': "{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'namespace': 'departments', 'LOG_NAME': LOG_NAME},
        dag=dag,
    )

    skip_recursive_call_task = DummyOperator(
        task_id='skip_recursive_call',
        dag=dag)

    execute_recursive_call_task = TriggerDagRunOperator(
        task_id='execute_recursive_call_task',
        trigger_dag_id='location_scd_dag',
        dag=dag
    )

    
    generate_data >> check_stream_state_task >> find_file_to_process_task >> load_csv >> landing_to_staging >> staging_to_target
    staging_to_target >> archive_file >> check_recursive_task >> skip_recursive_call_task

    generate_data >> check_stream_state_task >> find_file_to_process_task >> load_csv >> landing_to_staging >> staging_to_target
    staging_to_target >> archive_file >> check_recursive_task >> execute_recursive_call_task

    generate_data >> check_stream_state_task >> find_file_to_process_task >> nothing_to_process_task
