from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

def define_file(ti):
    conn = GoogleCloudStorageHook()
    files = conn.list(Variable.get("BUCKET_ID"), prefix='persons/', delimiter='.csv')
    if len(files) > 1:
        ti.xcom_push(key='repeat', value=True)
    print(files[0])
    return files[0]