from datetime import datetime
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from google.cloud.logging_v2.logger import Logger
from google.cloud import bigquery
from dist.logger import get_logger
from dist.tables import fk_passage_table_name, datamart_throughput_table_name


def message_logging(logger, severity, message, **kwargs):
    logger.log_struct({
        'message': message,
        'run_id': kwargs['dag_run'].run_id
    },
    severity=severity)


def define_file(ti, **kwargs):
    path = kwargs['path']
    conn = GoogleCloudStorageHook()
    files = conn.list(Variable.get("BUCKET_ID"), prefix=path, delimiter='.csv')
    files.sort(key=lambda file_name: file_name.split('_')[-1]) # sort by data

    if len(files) > 1:
        ti.xcom_push(key='repeat', value=True)

    return files[0] if len(files) and not files[0] == path else None


def check_file_existing(ti, **kwargs):
    logger = get_logger(kwargs['LOG_NAME'])
    object_for_processing = ti.xcom_pull(task_ids='define_file_for_uploading')

    if object_for_processing:
        logger.log_struct({
            'message': f"{datetime.now(tz=None)} Detected object {object_for_processing} for processing in {kwargs['namespace']} job {kwargs['dag_run'].run_id}",
            'run_id': kwargs['dag_run'].run_id,
            'object_for_processing': object_for_processing
        }, severity="INFO")

        return 'gcs_to_bigquery'

    logger.log_struct({
        'message': f"{datetime.now(tz=None)} Nothing to process for {kwargs['namespace']} during job {kwargs['dag_run'].run_id}",
        'run_id': kwargs['dag_run'].run_id
    }, severity="WARNING")

    return 'end_of_job'

def check_more_files(ti, **kwargs):
    logger = get_logger(kwargs['LOG_NAME'])
    rerun_dag = ti.xcom_pull(task_ids='define_file_for_uploading', key='repeat')

    logger.log_struct({
        'message': f"{datetime.now(tz=None)} Processed object {kwargs['source_object']} was successfully moved to {kwargs['destination_object']}",
        'run_id': kwargs['dag_run'].run_id,
        'source_object': kwargs['source_object'],
        'destination_object': kwargs['destination_object'],
        'namespace': kwargs['namespace']
    }, severity="INFO")

    if rerun_dag:
        logger.log_struct({
             'message': f"{datetime.now(tz=None)} There are one or more objects in {kwargs['namespace']}/ folder. Dag will run again automatically",
             'run_id': kwargs['dag_run'].run_id
            }, severity="INFO")

        return kwargs.get('task_id', None) or 'rerun_dag'

    logger.log_struct({
             'message': f"{datetime.now(tz=None)} All objects in {kwargs['namespace']}/ folder were processed",
             'run_id': kwargs['dag_run'].run_id
            }, severity="INFO")

    return 'end_of_job'


def get_older_files(ti, **kwargs):
    conn = GoogleCloudStorageHook()
    prefix = 'processed_{0}'

    dimensions = ['gates', 'locations', 'persons', 'passcards', 'departments']

    for dim in dimensions:
        files = conn.list(Variable.get("BUCKET_ID"), prefix=prefix.format(dim), delimiter='.csv')
        files.sort(key=lambda file_name: file_name.split('_')[-1], reverse=True) # sort by date

        if len(files):
            ti.xcom_push(key=prefix.format(dim), value=files[0])
    return


def log_info(logger: Logger, message: str, run_id: str):
    logger.log_struct(
        {
            'message': message,
            'run_id': run_id,
        },
        severity='INFO',
    )
    return

def last_datamarts_updates(ti, **kwargs):
    client = bigquery.Client()

    tables = [datamart_throughput_table_name]   # ADD ALL DATAMARTS
    for table in tables:
        query_job = client.query(f"""
            SELECT MAX(timestamp) as max_time
            FROM {Variable.get("DATAMART_DATASET_ID")}.{table};
        """)
        result = [i for i in query_job]
        if result[0].max_time:
            ti.xcom_push(key=table, value=str(result[0].max_time))
        else:
            ti.xcom_push(key=table, value='1800-01-01')
    return


def get_last_updated_record(ti, **kwargs):
    client = bigquery.Client()
    query_job = client.query(f"""
        SELECT MAX(timestamp) as max_time
        FROM {Variable.get("DATASET_ID")}.{fk_passage_table_name};
    """)
    result = [i for i in query_job]
    if result[0].max_time:
        ti.xcom_push(key='last_updated_date', value=str(result[0].max_time))
    else:
        ti.xcom_push(key='last_updated_date', value='1800-01-01')
    return