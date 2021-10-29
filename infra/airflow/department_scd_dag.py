import airflow
from airflow.models.dag import DagModel
from dist import tables
from datetime import datetime
from dist.logger import get_logger
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, task
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from dist.sql_queries import sql_landing_to_staging_dept, sql_staging_to_target_dept, sql_temporary_to_landing_dept
from dist.utils import define_file, check_file_existing, check_more_files, message_logging


LOG_NAME = 'department-dag'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'department_scd_dag',
        start_date=datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    start_of_job = PythonOperator(
        task_id='logging_start_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', f'{datetime.now(tz=None)} Starting of job with id {{{{run_id}}}} for processing department data'],
        dag=dag
    )

    define_file_to_process = PythonOperator(
        task_id='define_file_for_uploading',
        python_callable=define_file,
        op_kwargs={'path': 'departments/'},
        dag=dag,
    )

    check_file = BranchPythonOperator(
        task_id='check_file_existing',
        python_callable=check_file_existing,
        op_kwargs={'namespace': 'departments', 'LOG_NAME': LOG_NAME},
        dag=dag,
    )

    end_of_job = PythonOperator(
        task_id='end_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', str(datetime.now(tz=None)) + ' Job with id {{run_id}} ended'],
        trigger_rule='all_done',
        dag=dag
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=Variable.get("BUCKET_ID"),
        source_objects=[
            "{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_project_dataset_table=f"{Variable.get('LANDING_DATASET_ID')}.{tables.tmp_dept_table_name}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'department_key', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'building_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'parent_id', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        retries=0,
        dag=dag,
    )
    
    log_on_success_file_to_tmp = PythonOperator(
        task_id='log_on_success',
        python_callable=message_logging,
        op_args=[logger, 'INFO', 
            f"{datetime.now(tz=None)} Successfuly loaded data from {{{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}}} to {Variable.get('LANDING_DATASET_ID')}.{tables.tmp_dept_table_name}. Job id - {{{{run_id}}}}"],
        dag=dag
    )

    log_on_failure_file_to_tmp = PythonOperator(
        task_id='log_on_failure',
        python_callable=message_logging,
        op_args=[logger, 'ERROR', 
            f"{datetime.now(tz=None)} Error while loading file {{{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}}} to {Variable.get('LANDING_DATASET_ID')}.{tables.tmp_dept_table_name}. Job id - {{{{run_id}}}}"],
        trigger_rule='one_failed',
        dag=dag
    )
    
    temporary_to_landing = BigQueryOperator(
        dag=dag,
        task_id='temporary_to_landing',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        retries=0,
        sql=sql_temporary_to_landing_dept
    )

    log_on_success_tmp_to_lnd = PythonOperator(
        task_id='log_on_success_lnd',
        python_callable=message_logging,
        op_args=[logger, 'INFO', 
            f"{datetime.now(tz=None)} Successfuly loaded data from {Variable.get('LANDING_DATASET_ID')}.{tables.tmp_dept_table_name} to {Variable.get('LANDING_DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        dag=dag
    )

    log_on_failure_tmp_to_lnd = PythonOperator(
        task_id='log_on_failure_lnd',
        python_callable=message_logging,
        op_args=[logger, 'ERROR', 
            f"{datetime.now(tz=None)} Error while moving department data from {Variable.get('LANDING_DATASET_ID')}.{tables.tmp_dept_table_name} to {Variable.get('LANDING_DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        trigger_rule='one_failed',
        dag=dag
    )
    
    landing_to_staging = BigQueryOperator(
        dag=dag,
        task_id='landing_to_staging',
        location='US',
        use_legacy_sql=False,
        retries=0,
        write_disposition='WRITE_TRUNCATE',
        sql=sql_landing_to_staging_dept
    )

    log_on_success_lnd_to_stg = PythonOperator(
        task_id='log_on_success_stg',
        python_callable=message_logging,
        op_args=[logger, 'INFO', 
            f"{datetime.now(tz=None)} Successfuly loaded data from {Variable.get('LANDING_DATASET_ID')}.{tables.dept_table_name} to {Variable.get('STAGING_DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        dag=dag
    )

    log_on_failure_lnd_to_stg = PythonOperator(
        task_id='log_on_failure_stg',
        python_callable=message_logging,
        op_args=[logger, 'ERROR', 
            f"{datetime.now(tz=None)} Error while moving department data from {Variable.get('LANDING_DATASET_ID')}.{tables.dept_table_name} to {Variable.get('STAGING_DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        trigger_rule='one_failed',
        dag=dag
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        retries=0,
        write_disposition='WRITE_APPEND',
        sql=sql_staging_to_target_dept
    )

    log_on_success_stg_to_trg = PythonOperator(
        task_id='log_on_success_trg',
        python_callable=message_logging,
        op_args=[logger, 'INFO', 
            f"{datetime.now(tz=None)} Successfuly loaded data from {Variable.get('STAGING_DATASET_ID')}.{tables.dept_table_name} to {Variable.get('DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        dag=dag
    )

    log_on_failure_stg_to_trg = PythonOperator(
        task_id='log_on_failure_trg',
        python_callable=message_logging,
        op_args=[logger, 'ERROR', 
            f"{datetime.now(tz=None)} Error while moving department data from {Variable.get('STAGING_DATASET_ID')}.{tables.dept_table_name} to {Variable.get('DATASET_ID')}.{tables.dept_table_name}. Job id - {{{{run_id}}}}"],
        trigger_rule='one_failed',
        dag=dag
    )

    archive_file = GCSToGCSOperator(
        task_id='archive_file',
        source_bucket=Variable.get("BUCKET_ID"),
        source_objects=["{{ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_bucket=Variable.get("BUCKET_ID"),
        destination_object="processed_{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
        move_object=True
    )

    check_for_another_file = BranchPythonOperator(
        task_id='check_rerun_dag',
        python_callable=check_more_files,
        op_kwargs={'destination_object': "processed_{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'source_object': "{{ti.xcom_pull(task_ids='define_file_for_uploading')}}",
                   'namespace': 'departments', 'LOG_NAME': LOG_NAME},
        dag=dag,
    )

    rerun_dag = run_this = TriggerDagRunOperator(
        task_id='rerun_dag',
        trigger_dag_id='department_scd_dag',
        dag=dag
    )


    start_of_job >> define_file_to_process >> check_file >> end_of_job

    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_failure_file_to_tmp >> end_of_job

    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_success_file_to_tmp >> temporary_to_landing >> log_on_failure_tmp_to_lnd >> end_of_job

    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_success_file_to_tmp >> \
    temporary_to_landing >> log_on_success_tmp_to_lnd >> landing_to_staging >> log_on_failure_lnd_to_stg >> end_of_job
    
    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_success_file_to_tmp >> \
    temporary_to_landing >> log_on_success_tmp_to_lnd >> landing_to_staging >> log_on_success_lnd_to_stg >> \
    staging_to_target >> log_on_failure_stg_to_trg >> end_of_job

    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_success_file_to_tmp >> \
    temporary_to_landing >> log_on_success_tmp_to_lnd >> landing_to_staging >> log_on_success_lnd_to_stg >> \
    staging_to_target >> log_on_success_stg_to_trg >> archive_file >> check_for_another_file >> rerun_dag >> end_of_job

    start_of_job >> define_file_to_process >> check_file >> load_csv >> log_on_success_file_to_tmp >> \
    temporary_to_landing >> log_on_success_tmp_to_lnd >> landing_to_staging >> log_on_success_lnd_to_stg >> \
    staging_to_target >> log_on_success_stg_to_trg >> archive_file >> check_for_another_file >> end_of_job

    """

    start_of_job >> define_file_to_process >> check_file >> load_csv  >> \
    [log_on_success_file_to_tmp, log_on_failure_file_to_tmp] >> temporary_to_landing >> \
    [log_on_success_tmp_to_lnd, log_on_failure_tmp_to_lnd] >> landing_to_staging >> \
    [log_on_success_lnd_to_stg, log_on_failure_lnd_to_stg] >> end_of_job
    """