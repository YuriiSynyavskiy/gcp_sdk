import os
import re
import csv
import random
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import datetime


CONNECTION = GoogleCloudStorageHook()
HEADER = ['id', 'building_id', 'security_id', 'gate_id',
              'room_number', 'floor', 'description']
FILE_NAME = f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
BUCKET_NAME = 'edu-passage-bucket'
BLOB_NAME = f'locations/{FILE_NAME}'

def upload_to_bucket(file_name):
    """ Upload data to the bucket"""
    CONNECTION.upload(
        bucket_name = BUCKET_NAME,
        object_name = f'locations/{file_name}',
        filename = file_name
    )


def download_file(file_name):
    """ Download file from the bucket"""
    return CONNECTION.download(
        bucket_name = BUCKET_NAME,
        object_name = f'{file_name}',
        filename = file_name.split('/')[-1]
    )


def remove_file_from_gcs(file_name):
    """ Remove file from the bucket"""
    CONNECTION.delete(
        bucket_name = BUCKET_NAME,
        object_name = f'locations/{file_name}'
    )


def generate_room_number():
    room = random.randint(101, 599)
    floor = int(str(room)[0])
    if floor == 1:
        return room, floor, 'Hall'
    elif floor == 2:
        return room, floor, 'Back Yard'
    elif floor == 3:
        return room, floor, 'Developers Zone'
    elif floor == 4:
        return room, floor, 'Kitchen'
    elif floor == 5:
        return room, floor, 'Meeting Room'


def create_location_data():
    file_name = f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
    with open(file_name, 'w', newline='') as f:
        location_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                        fieldnames=HEADER)
        location_writer.writeheader()
        list_of_dict = []
        for i in range(1,21):
            room, floor, descriptions = generate_room_number()
            result = {"id": str(i), "building_id": random.randint(1,2), 
                      "security_id": random.randint(1,3), "gate_id": str(i), 
                      "room_number": room ,"floor": floor, "description": descriptions}
            list_of_dict.append(result)
        location_writer.writerows(list_of_dict)
    return file_name


def add_run_id(**kwargs):
    """Add run_id to the file from airflow"""
    file_path = download_file(kwargs['file_name'])
    file_name = file_path.split('/')[-1]
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
    remove_file_from_gcs(file_path)
