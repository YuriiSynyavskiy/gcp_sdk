import datetime
import airflow
from dist import tables
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from dist.sql_queries import sql_landing_to_staging_dept, sql_staging_to_target_dept
from dist.utils import define_file, check_file_existing, check_more_files


with airflow.DAG(
        'department_scd_dag',
        start_date=datetime.datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
    
    define_file_to_process = PythonOperator(
        task_id='define_file_for_uploading',
        python_callable=define_file,
        op_kwargs={'path': 'departments/'},
        dag=dag,
    )
    check_file = BranchPythonOperator(
        task_id='check_file_existing',
        python_callable=check_file_existing,
        dag=dag,
    )

    pass_operator = DummyOperator(task_id='pass', dag=dag)

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=Variable.get("BUCKET_ID"),
        source_objects=[
            "{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_project_dataset_table=f"{Variable.get('DATASET_ID')}.{tables.landing_dept_table}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'department_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'building_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'parent_id', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        dag=dag,
    )

    landing_to_staging = BigQueryOperator(
        dag=dag,
        task_id='landing_to_staging',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        sql=sql_landing_to_staging_dept
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=sql_staging_to_target_dept
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
        dag=dag,
    )

    rerun_dag = run_this = TriggerDagRunOperator(
        task_id='rerun_dag',
        trigger_dag_id='department_scd_dag',
        dag=dag
    )

 
    define_file_to_process >> check_file >> load_csv >> landing_to_staging >> staging_to_target >> archive_file >> check_for_another_file >> pass_operator
    define_file_to_process >> check_file >> load_csv >> landing_to_staging >> staging_to_target >> archive_file >> check_for_another_file >> rerun_dag
    define_file_to_process >> check_file >> pass_operator