import os
import datetime
import airflow
from datetime import datetime
from dist.logger import get_logger
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dist.utils import get_older_files, message_logging
from dist.file_to_gcs import LocalToGCSOperator
from data_generating.department_data import create_department_data, update_department_data
from data_generating.person_data import create_person_data, update_person_data
from data_generating.location_data import create_location_data
from data_generating.gate_data import create_gate_data
from dist.paths import (departments_folder, departments_archive_folder, persons_folder, persons_archive_folder,
                        locations_folder, gates_folder)


LOG_NAME = 'generating-dag'
logger = get_logger(LOG_NAME)


with airflow.DAG(
        'generating_dag',
        start_date=datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    start_of_job = PythonOperator(
        task_id='logging_start_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', f'{datetime.now(tz=None)} Starting of job with id {{{{run_id}}}} for generating dummy data'],
        dag=dag
    )

    define_file_to_process = PythonOperator(
        task_id='get_older_files',
        python_callable=get_older_files,
        dag=dag,
    )

    generate_department_data = LocalToGCSOperator(
        task_id='generate_department_data',
        bucket_name=Variable.get("BUCKET_ID"),
        bucket_folder=departments_folder,
        previous_file=f'{{{{ti.xcom_pull(task_ids="get_older_files", key="{departments_archive_folder}")}}}}',
        create_function=create_department_data,
        update_function=update_department_data,
        logger=logger,
        dag=dag
    )

    generate_person_data = LocalToGCSOperator(
        task_id='generate_person_data',
        bucket_name=Variable.get("BUCKET_ID"),
        bucket_folder=persons_folder,
        previous_file=f'{{{{ti.xcom_pull(task_ids="get_older_files", key="{persons_archive_folder}")}}}}',
        create_function=create_person_data,
        update_function=update_person_data,
        logger=logger,
        dag=dag
    )

    generate_location_data = LocalToGCSOperator(
        task_id='generate_location_data',
        bucket_name=Variable.get("BUCKET_ID"),
        bucket_folder=locations_folder,
        create_function=create_location_data,
        logger=logger,
        run_id=True,
        dag=dag
    )

    generate_gate_data = LocalToGCSOperator(
        task_id='generate_gate_data',
        bucket_name=Variable.get("BUCKET_ID"),
        bucket_folder=gates_folder,
        create_function=create_gate_data,
        logger=logger,
        run_id=True,
        dag=dag
    )

    end_of_job = PythonOperator(
        task_id='end_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', str(datetime.now(tz=None)) + ' Job with id {{run_id}} ended'],
        trigger_rule='all_done',
        dag=dag
    )
    
    start_of_job >> define_file_to_process >> generate_department_data >> generate_person_data >> \
    generate_location_data >> generate_gate_data >> end_of_job
