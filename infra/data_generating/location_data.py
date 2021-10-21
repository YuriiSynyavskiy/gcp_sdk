import os
import csv
import random
import argparse
from google.cloud import storage
from datetime import datetime

HEADER = ['run_id', 'id', 'building_id', 'security_id', 'gate_id',
              'room_number', 'floor', 'description']
FILE_PATH = f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'


def upload_to_bucket(blob_name, bucket_name):
    """ Upload data to a bucket"""

    client = storage.Client.from_service_account_json(json_credentials_path='/home/airflow/gcs/dags/data_generating/storage.json')

    # Creating bucket object
    bucket = client.get_bucket(bucket_name)
    
    object_name_in_gcs_bucket = bucket.blob(blob_name)
    object_name_in_gcs_bucket.upload_from_filename(FILE_PATH)


def generate_room_number():
    room = random.randint(101, 501)
    floor = int(str(room)[0])
    return room, floor


def generate_description():
    descriptions = ['Developers Zone', 'Kitchen', 'Meeting Room', 'Hall', 'Back Yard']
    return random.choice(descriptions)


def create_data(**kwargs):
    bucket_name = 'edu-passage-bucket'
    blob_name = f'locations/{FILE_PATH}'
    with open(FILE_PATH, 'w', newline='') as f:
        location_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                        fieldnames=HEADER)
        location_writer.writeheader()
        list_of_dict = []
        for i in range(1,51):
            room, floor = generate_room_number()
            result = {"run_id": kwargs['dag_run'].run_id, "id": str(i), "building_id": random.randint(1,2), 
                      "security_id": random.randint(1,10), "gate_id": random.randint(1,50), 
                      "room_number": room ,"floor": floor, "description": generate_description()}
            list_of_dict.append(result)
        location_writer.writerows(list_of_dict)
    upload_to_bucket(blob_name, bucket_name)


def update_data():
    try:
        with open(FILE_PATH, 'r') as f_r:
            location_reader = csv.reader(f_r, delimiter=',', quotechar='"')
            updated_file_name = f"locations_{FILE_PATH.split('_')[-1].split('.')[0]}_updated_{str(datetime.timestamp(datetime.now())).split('.')[0]}.csv"
            with open(updated_file_name, 'w') as f_w:
                location_writer = csv.writer(f_w, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                location_writer.writerow(HEADER)
                skip_header = next(location_reader)
                for row in location_reader:
                    if random.random() < 0.1:
                        room, floor = generate_room_number()
                        description = generate_description()
                        choice = random.randint(1,7)
                        # Change building_id
                        if choice == 2:  
                            row[1] = random.randint(1,20)
                        # Change security_id
                        elif choice == 3:            
                            row[2] = random.randint(1,10)
                        # Change gate_id
                        elif choice == 4:            
                            row[3] = random.randint(1,50)
                        # Change room_number
                        elif choice == 5:
                            row[4] = room
                        # Change floor
                        elif choice == 6:
                            row[5] = floor
                        # Change description
                        elif choice == 7:
                            row[6] = description
                    location_writer.writerow(row)
    except FileNotFoundError:
        print("Incorrect file name!")
