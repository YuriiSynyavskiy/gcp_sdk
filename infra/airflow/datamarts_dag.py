import airflow
from datetime import datetime
from dist.logger import get_logger
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from dist.utils import message_logging, last_datamarts_updates
from dist.sql_queries import sql_update_throughput_datamart, sql_update_gate_analysis_datamart

LOG_NAME = 'datamarts-dag'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'datamarts_dag',
        start_date=datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
    
    start_of_job = PythonOperator(
        task_id='logging_start_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', f"{datetime.now(tz=None)} Starting of job with id {{{{run_id}}}} for updating datamart's data"],
        dag=dag,
    )
    
    get_last_datamarts_updates = PythonOperator(
        task_id='get_last_datamarts_updates',
        python_callable=last_datamarts_updates,
        dag=dag,
    )

    throughput_datamart = BigQueryOperator(
        dag=dag,
        task_id='throughput_datamart',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        retries=0,
        sql=sql_update_throughput_datamart
    )

    gate_analysis_datamart = BigQueryOperator(
        dag=dag,
        task_id='gate_analysis_datamart',
        location='US',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        retries=0,
        sql=sql_update_gate_analysis_datamart
    )

    end_of_job = PythonOperator(
        task_id='end_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', str(datetime.now(tz=None)) + ' Job with id {{run_id}} ended'],
        trigger_rule='all_done',
        dag=dag
    )

    start_of_job >> get_last_datamarts_updates >> throughput_datamart >> gate_analysis_datamart >> end_of_job
