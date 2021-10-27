import os
import csv
import names
import random
from datetime import datetime


def phn():
    n = '0000000000'
    while '9' in n[3:6] or n[3:6]=='000' or n[6]==n[7]==n[8]==n[9]:
        n = str(random.randint(10**9, 10**10-1))
    return n[:3] + '-' + n[3:6] + '-' + n[6:]


def create_person_data():
    """Generate persons' data"""
    file_name = f'persons_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv'
    with open(file_name, 'w') as f:
        person_writer = csv.DictWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, 
                                       fieldnames=["id", "department_id", "position_id", "name", 
                                                   "surname", "salary", "phone", "start_date", "end_date"])
        person_writer.writeheader()
        
        person_writer.writerows({"id": str(i), "department_id": random.randint(1,20), "position_id": random.randint(1,8), "name": names.get_first_name(),
                                 "surname": names.get_last_name(), "salary": random.randint(2,14)*random.choice([1000, 1250]), "phone": phn(), 
                                 "start_date": f"{random.randint(1980, 2021)}-{random.randint(1,12)}-{random.randint(1,28)}"} for i in range(1,101))
    return file_name     

def update_person_data(file_name):
    with open(file_name, 'r') as f_r:
        person_reader = csv.reader(f_r, delimiter=',', quotechar='"')
        updated_file_name = f"persons_{file_name.split('_')[-1].split('.')[0]}_updated_{str(datetime.timestamp(datetime.now())).split('.')[0]}.csv"
        with open(updated_file_name, 'w') as f_w:
            person_writer = csv.writer(f_w, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            person_writer.writerow(["id", "department_id", "position_id", "name", 
                                    "surname", "salary", "phone", "start_date", "end_date"])
            skip_header = next(person_reader)
            for row in person_reader:
                if random.random() < 0.1:
                    choice = random.randint(1,7)

                    # Change dept_id
                    if choice == 1:  
                        row[1] = random.randint(1,20)
                    # Change position_id
                    elif choice == 2:            
                        row[2] = random.randint(1,8)
                    # Change name
                    elif choice == 3:            
                        row[3] = names.get_first_name()
                    # Change surname
                    elif choice == 4:
                        row[4] = names.get_last_name()
                    # Change salary
                    elif choice == 5:
                        row[5] = random.randint(2,14)*random.choice([1000, 1250])
                    # Change phone
                    elif choice == 6:
                        row[6] = phn()
                person_writer.writerow(row)
    return updated_file_name

