from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

def define_file(ti, **kwargs):
    path = kwargs['path']
    conn = GoogleCloudStorageHook()
    files = conn.list(Variable.get("BUCKET_ID"), prefix=path, delimiter='.csv')
    files.sort(key=lambda file_name: file_name.split('_')[-1], reverse=True) # sort by data
    if len(files) > 1:
        ti.xcom_push(key='repeat', value=True)
    return files[0] if len(files) and not files[0] == path else None

def check_file_existing(ti):
    if ti.xcom_pull(task_ids='define_file_for_uploading'):
        return 'gcs_to_bigquery'
    return 'pass'

def check_more_files(ti):
    if ti.xcom_pull(task_ids='define_file_for_uploading', key='repeat'):
        return 'rerun_dag'
    return 'pass'