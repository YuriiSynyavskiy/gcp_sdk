import airflow
from datetime import datetime
from dist.logger import get_logger
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from dist.utils import message_logging, get_last_updated_record
from dist.sql_queries import sql_landing_to_staging_fk_passage, sql_staging_to_target_fk_passage

LOG_NAME = 'fk-passage-dag'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'fk_passage_dag',
        start_date=datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
    
    start_of_job = PythonOperator(
        task_id='logging_start_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', f"{datetime.now(tz=None)} Starting of job with id {{{{run_id}}}} for enriching fact table"],
        dag=dag,
    )
    get_last_updated_record_task = PythonOperator(
        task_id='get_last_updated_record',
        python_callable=get_last_updated_record,
        dag=dag,
    )

    landing_to_staging = BigQueryOperator(
        dag=dag,
        task_id='landing_to_staging',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        retries=0,
        sql=sql_landing_to_staging_fk_passage
    )

    staging_to_target = BigQueryOperator(
        dag=dag,
        task_id='staging_to_target',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        retries=0,
        sql=sql_staging_to_target_fk_passage
    )

    end_of_job = PythonOperator(
        task_id='end_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', str(datetime.now(tz=None)) + ' Job with id {{run_id}} ended'],
        trigger_rule='all_done',
        dag=dag
    )

    start_of_job  >> get_last_updated_record_task >> landing_to_staging >> staging_to_target >> end_of_job