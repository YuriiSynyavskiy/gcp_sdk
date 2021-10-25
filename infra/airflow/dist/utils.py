from datetime import date, datetime
from dist.logger import get_logger
from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

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
