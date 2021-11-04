import airflow
from datetime import datetime
from dist.logger import get_logger
from dist.utils import message_logging
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor


LOG_NAME = 'trigger_scd_dags'
logger = get_logger(LOG_NAME)

with airflow.DAG(
        'trigger_scd_dags',
        start_date=datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:
    
    start_of_job = PythonOperator(
        task_id='logging_start_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', f'{datetime.now(tz=None)} Starting of job with id {{{{run_id}}}} for trigger scd DAGs'],
        dag=dag
    )

    execute_person_scd_dag = TriggerDagRunOperator(
        task_id='execute_person_scd_dag',
        trigger_dag_id='person_scd_dag',
        dag=dag
    )

    person_scd_dag_sensor = ExternalTaskSensor(
        task_id='person_scd_dag_sensor',
        external_dag_id='person_scd_dag',
        external_task_id='end_of_job',
        dag=dag
    )

    execute_passcard_scd_dag = TriggerDagRunOperator(
        task_id='execute_passcard_scd_dag',
        trigger_dag_id='passcard_scd_dag',
        dag=dag
    )

    passcard_scd_dag_sensor = ExternalTaskSensor(
        task_id='passcard_scd_dag_sensor',
        external_dag_id='passcard_scd_dag',
        external_task_id='finish',
        dag=dag
    )

    execute_department_scd_dag = TriggerDagRunOperator(
        task_id='execute_department_scd_dag',
        trigger_dag_id='department_scd_dag',
        dag=dag
    )

    department_scd_dag_sensor = ExternalTaskSensor(
        task_id='department_scd_dag_sensor',
        external_dag_id='department_scd_dag',
        external_task_id='end_of_job',
        dag=dag
    )

    execute_gate_scd_dag = TriggerDagRunOperator(
        task_id='execute_gate_scd_dag',
        trigger_dag_id='gate_scd_dag',
        dag=dag
    )

    gate_scd_dag_sensor = ExternalTaskSensor(
        task_id='gate_scd_dag_sensor',
        external_dag_id='gate_scd_dag',
        external_task_id='end_of_job',
        dag=dag
    )

    execute_location_scd_dag = TriggerDagRunOperator(
        task_id='execute_location_scd_dag',
        trigger_dag_id='location_scd_dag',
        dag=dag
    )

    location_scd_dag_sensor = ExternalTaskSensor(
        task_id='location_scd_dag_sensor',
        external_dag_id='location_scd_dag',
        external_task_id='end_of_job',
        dag=dag
    )

    execute_fact_dag = TriggerDagRunOperator(
        task_id='execute_fact_dag',
        trigger_dag_id='fk_passage_dag',
        dag=dag
    )

    fact_dag_sensor = ExternalTaskSensor(
        task_id='fact_dag_sensor',
        external_dag_id='fk_passage_dag',
        external_task_id='end_of_job',
        dag=dag
    )

    execute_datamarts_dag = TriggerDagRunOperator(
        task_id='execute_datamarts_dag',
        trigger_dag_id='datamarts_dag',
        dag=dag
    )

    datamarts_dag_sensor = ExternalTaskSensor(
        task_id='datamarts_dag_sensor',
        external_dag_id='datamarts_dag',
        external_task_id='end_of_job',
        dag=dag
    )


    end_of_job = PythonOperator(
        task_id='end_of_job',
        python_callable=message_logging,
        op_args=[logger, 'INFO', str(datetime.now(tz=None)) + ' Job with id {{run_id}} ended'],
        trigger_rule='all_done',
        dag=dag
    )
    start_of_job >> execute_person_scd_dag >> person_scd_dag_sensor >> execute_passcard_scd_dag >> passcard_scd_dag_sensor >> \
    execute_department_scd_dag >> department_scd_dag_sensor >> execute_gate_scd_dag >> gate_scd_dag_sensor >> \
    execute_location_scd_dag >> location_scd_dag_sensor >> execute_fact_dag >> fact_dag_sensor >> \
    execute_datamarts_dag >> datamarts_dag_sensor >> end_of_job
