import os
import datetime
from re import DEBUG
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
from dist.sql_queries import sql_landing_to_staging_gate, sql_staging_to_target_gate
from dist.utils import define_file, check_file_existing, check_more_files
from data_generating.gate_data import create_data, add_run_id

LOG_NAME = 'gate-dag'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'gate_scd_dag',
        start_date=datetime.datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
        
    create_data_task = PythonOperator(
        task_id='create_data_task',
        python_callable=create_data,
        dag=dag
    )

    define_file_for_uploading = PythonOperator(
        task_id='define_file_for_uploading',
        python_callable=define_file,
        op_kwargs={'path': 'gates/'},
        dag=dag,
    )

    check_stream_state_task = BranchPythonOperator(
        task_id='check_stream_state',
        python_callable=check_file_existing,
        op_kwargs={'namespace': 'gate', 'LOG_NAME': LOG_NAME},
        dag=dag,
    )

    add_run_id_column_task = PythonOperator(
        task_id='add_run_id_column',
        python_callable=add_run_id
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=Variable.get("BUCKET_ID"),
        source_objects=[
            "{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_project_dataset_table=f"{Variable.get('LANDING_DATASET_ID')}.{tables.gate_table_name}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'gate_key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'contact_information', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'throughput', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'run_id', 'type': 'STRING', 'mode': 'NULLABLE'}
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
        sql=sql_landing_to_staging_gate
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=sql_staging_to_target_gate
    )

    archive_file = GCSToGCSOperator(
        task_id='archive_file',
        source_bucket=Variable.get("BUCKET_ID"),
        source_objects=["{{ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_bucket=Variable.get("BUCKET_ID"),
        destination_object="processed_{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
        move_object=True
    )

    check_recursive_task = BranchPythonOperator(
        task_id='check_recursive_task',
        python_callable=check_more_files,
        op_kwargs={'destination_object': "processed_{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'source_object': "{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'namespace': 'departments', 'LOG_NAME': LOG_NAME, 'task_id': 'execute_recursive_call_task'},
        dag=dag,
    )

    skip_recursive_call_task = DummyOperator(
        task_id='end_of_job',
        dag=dag)

    execute_recursive_call_task = TriggerDagRunOperator(
        task_id='execute_recursive_call_task',
        trigger_dag_id='gate_scd_dag',
        dag=dag
    )

    create_data_task >> add_run_id_column_task >> define_file_for_uploading >> check_stream_state_task >> load_csv >> landing_to_staging >> staging_to_target
    staging_to_target >> archive_file >> check_recursive_task >> skip_recursive_call_task

    create_data_task >> add_run_id_column_task >> define_file_for_uploading >> check_stream_state_task >> load_csv >> landing_to_staging >> staging_to_target
    staging_to_target >> archive_file >> check_recursive_task >> execute_recursive_call_task

    create_data_task >> add_run_id_column_task >> define_file_for_uploading >> check_stream_state_task >> skip_recursive_call_task