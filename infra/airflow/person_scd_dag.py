import datetime
import airflow
from person_scd_define_file import define_file
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator


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
        source_objects=["{{ ti.xcom_pull(task_ids='define_file_for_uploading')}}"],
        destination_project_dataset_table=f"{Variable.get('DATASET_ID')}.landing_dm_person",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect = True,
        dag=dag,
    )

    landing_to_staging = 

    define_file_to_process >> load_csv >> landing_to_staging