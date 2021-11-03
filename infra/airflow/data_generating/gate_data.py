import os
import re
import csv
import random
from google.cloud import storage
from datetime import datetime

CLIENT = storage.Client.from_service_account_json(json_credentials_path="/home/airflow/gcs/dags/data_generating/storage.json")
HEADER = ['id', 'contact_information', 'state', 'throughput']
FILE_NAME = f'gates_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
BUCKET_NAME = 'edu-passage-bucket'
BLOB_NAME = f'gates/{FILE_NAME}'


def upload_to_bucket(file_path):
    """ Upload data to a bucket"""
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    
    object_name_in_gcs_bucket = bucket.blob(BLOB_NAME)
    object_name_in_gcs_bucket.upload_from_filename(file_path)


def create_gate_data():
    """Generate gates' data"""
    with open(FILE_NAME, 'w', newline='') as f:
        location_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                        fieldnames=HEADER)
        location_writer.writeheader()
        list_of_dict = []
        for i in range(1,21):
            result = {"id": str(i), "contact_information": f'{random.randint(100, 999)}-{random.randint(100,999)}-{random.randint(1000,9999)}', 
                      "state": 'working', "throughput": 10 * random.randint(3,6)}
            list_of_dict.append(result)
        location_writer.writerows(list_of_dict)
    return FILE_NAME


def remove_file_from_gcs(blob_name):
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    print(blob)
    blob.delete()


def add_run_id(**kwargs):
    """Add run_id to the file from airflow"""
    list_of_blobs = []
    for blob in CLIENT.list_blobs(BUCKET_NAME, prefix='gates'):
        list_of_blobs.append(blob)
    print(list_of_blobs)
    blob = re.search(r'gates/gates.*\.csv', str(list_of_blobs[1]))
    blob = blob.group()
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    get_blob_name = bucket.blob(blob)
    download_blob = get_blob_name.download_as_string().decode("utf-8")
    with open(FILE_NAME, 'w') as f:
        lns = download_blob.split('\n')
        for item in lns:
            f.write(item)
    file_name = [filename for filename in os.listdir('.') if filename.startswith("gates")][0]
    new_file_name = f'new_{file_name}'
    with open(file_name, 'r') as read_obj, \
            open(new_file_name, 'w', newline='') as write_obj:
        csv_reader = csv.reader(read_obj)
        csv_writer = csv.writer(write_obj)
        for row in csv_reader:
            transform_row = lambda row, line_num: row.append('run_id') if line_num == 1 else row.append(
                       kwargs['dag_run'].run_id)
            transform_row(row, csv_reader.line_num)
            csv_writer.writerow(row)
    upload_to_bucket(new_file_name)
    os.remove(new_file_name)
    os.remove(file_name)
    remove_file_from_gcs(blob)
