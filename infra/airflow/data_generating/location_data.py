import os
import re
import csv
import random
from google.cloud import storage
from datetime import datetime

CLIENT = storage.Client.from_service_account_json(json_credentials_path="/home/airflow/gcs/dags/data_generating/storage.json")
HEADER = ['id', 'building_id', 'security_id', 'gate_id',
              'room_number', 'floor', 'description']
FILE_NAME = f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
BUCKET_NAME = 'edu-passage-bucket'
BLOB_NAME = f'locations/{FILE_NAME}'

def upload_to_bucket(file_path):
    """ Upload data to a bucket"""
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    
    object_name_in_gcs_bucket = bucket.blob(BLOB_NAME)
    object_name_in_gcs_bucket.upload_from_filename(file_path)


def generate_room_number():
    room = random.randint(101, 501)
    floor = int(str(room)[0])
    return room, floor


def generate_description():
    descriptions = ['Developers Zone', 'Kitchen', 'Meeting Room', 'Hall', 'Back Yard']
    return random.choice(descriptions)


def create_location_data():
    file_name = f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
    with open(file_name, 'w', newline='') as f:
        location_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                        fieldnames=HEADER)
        location_writer.writeheader()
        list_of_dict = []
        for i in range(1,21):
            room, floor = generate_room_number()
            result = {"id": str(i), "building_id": random.randint(1,2), 
                      "security_id": random.randint(1,10), "gate_id": random.randint(1,50), 
                      "room_number": room ,"floor": floor, "description": generate_description()}
            list_of_dict.append(result)
        location_writer.writerows(list_of_dict)
    return file_name


def remove_file_from_gcs(blob_name):
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    print(blob)
    blob.delete()


def add_run_id(**kwargs):
    list_of_blobs = []
    for blob in CLIENT.list_blobs(BUCKET_NAME, prefix='locations'):
        list_of_blobs.append(blob)
    blob = re.search(r'locations/locations.*\.csv', str(list_of_blobs[1]))
    blob = blob.group()
    bucket = CLIENT.get_bucket(BUCKET_NAME)
    get_blob_name = bucket.blob(blob)
    download_blob = get_blob_name.download_as_string().decode("utf-8")
    with open(FILE_NAME, 'w') as f:
        lns = download_blob.split('\n')
        for item in lns:
            f.write(item)
    file_name = [filename for filename in os.listdir('.') if filename.startswith("locations")][0]
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
