import datetime
import airflow
from person_dist import tables
from person_dist.person_scd_define_file import define_file
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from person_dist.sql_queries import sql_landing_to_staging, sql_staging_to_target

with airflow.DAG(
        'person_scd_dag',
        start_date=datetime.datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    define_file_to_process = PythonOperator(
        task_id='define_file_for_uploading',
        python_callable=define_file
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=Variable.get("BUCKET_ID"),
        source_objects=[
            "{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_project_dataset_table=f"{Variable.get('DATASET_ID')}.{tables.landing_table}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'person_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'department_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'position_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'surname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'salary', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'start_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'end_date', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        dag=dag,
    )
    
    landing_to_staging = BigQueryOperator(
        dag=dag,
        task_id='landing_to_staging',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        sql=sql_landing_to_staging
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=sql_staging_to_target
    )

    # archive_file
    define_file_to_process >> load_csv >> landing_to_staging >> staging_to_target # >> archive_file
