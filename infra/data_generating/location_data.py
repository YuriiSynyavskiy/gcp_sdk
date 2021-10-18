import os
import csv
import random
import argparse
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime

HEADER = ['id', 'building_id', 'security_id', 'gate_id',
              'room_number', 'floor', 'description']


def generate_room_number():
    room = random.randint(101, 501)
    floor = int(str(room)[0])
    return room, floor


def generate_description():
    descriptions = ['Developers Zone', 'Kitchen', 'Meeting Room', 'Hall', 'Back Yard']
    return random.choice(descriptions)


def create_data(file_path):
    with open(file_path, 'w') as f:
        location_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                        fieldnames=HEADER)
        location_writer.writeheader()
        list_of_dict = []
        for i in range(1,11):
            room, floor = generate_room_number()
            result = {"id": str(i), "building_id": random.randint(1,20), 
                                            "security_id": random.randint(1,10), "gate_id": random.randint(1,50), 
                                            "room_number": room ,"floor": floor, "description": generate_description()}
            list_of_dict.append(result)
        location_writer.writerows(list_of_dict)


def update_data(file_path):
    try:
        with open(file_path, 'r') as f_r:
            location_reader = csv.reader(f_r, delimiter=',', quotechar='"')
            updated_file_name = f"locations_{args.file_path.split('_')[-1].split('.')[0]}_updated_{str(datetime.timestamp(datetime.now())).split('.')[0]}.csv"
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
                        if choice == 1:  
                            row[1] = random.randint(1,20)
                        # Change security_id
                        elif choice == 2:            
                            row[2] = random.randint(1,10)
                        # Change gate_id
                        elif choice == 3:            
                            row[3] = random.randint(1,50)
                        # Change room_number
                        elif choice == 4:
                            row[4] = room
                        # Change floor
                        elif choice == 5:
                            row[5] = floor
                        # Change description
                        elif choice == 6:
                            row[6] = description
                    location_writer.writerow(row)
    except FileNotFoundError:
        print("Incorrect file name!")


def main(type, file_path, upload):
    if type == 'create':
        create_data(file_path)
    else:
        update_data(file_path)


if __name__=="__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Generate locations data")
    parser.add_argument('--type', choices=['create', 'update'], type=str, default='create')
    parser.add_argument('--file_path', type=str, help='Path for file to update', default=f'locations_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv')
    parser.add_argument('--upload', choices=[True, False], type=bool, help='Upload file to bucket')

    args = parser.parse_args()
    type = args.type
    file_path = args.file_path
    upload = args.upload
    main(type, file_path, upload)
