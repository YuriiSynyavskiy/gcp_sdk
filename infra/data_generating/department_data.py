import os
import csv
import names
import random
import argparse
from dotenv import load_dotenv
from datetime import datetime
from essential_generators import DocumentGenerator

available_depts_names = ["Marketing Department", "Operations Department", "Finance Department", 
                        "Sales Department", "Human Resource Department", "Purchase Department", 
                        "Production Department", "R&D Department", "Accounting Department",
                        "Quality Control Department", "Big Data Department", "Strategy Department",
                        "Technology Department", "Equipment Department", "Quality Assuarance",
                        "Packaging Department", "Medical Department", "Analytical Department"]

if __name__=="__main__":
    load_dotenv()
    gen = DocumentGenerator()
    parser = argparse.ArgumentParser(description="Generate departments' data")
    parser.add_argument('--type', choices=['create', 'update'], type=str, default='create')
    parser.add_argument('--file_path', type=str, help='Path for file to update', default=f'departments_{str(datetime.timestamp(datetime.now())).split(".")[0]}.csv')
    parser.add_argument('--upload', choices=[True, False], type=bool, help='Upload file to bucket')

    args = parser.parse_args()

    if args.type == 'create':
        with open(args.file_path, 'w') as f:
            dept_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            dept_writer.writerow(["id", "building_id", "name", "description", "parent_id"])
            dept_writer.writerow(["1", random.randint(1,20), "Organization managment", gen.sentence(), ''])
            dept_writer.writerow(["2", random.randint(1,20), "General managment", gen.sentence(), "1"])
            for i in range(3,21):
                random_name = available_depts_names.pop(random.randint(0, len(available_depts_names)-1))

                dept_writer.writerow([str(i), random.randint(1,2), random_name, gen.sentence(), (random.randint(1,3) if not i == 3 else random.randint(1,2))])


    else:
        try:
            with open(args.file_path, 'r') as f_r:
                dept_reader = csv.reader(f_r, delimiter=',', quotechar='"')
                updated_file_name = f"departments_{args.file_path.split('_')[-1].split('.')[0]}_updated_{str(datetime.timestamp(datetime.now())).split('.')[0]}.csv"
                with open(updated_file_name, 'w') as f_w:
                    dept_writer = csv.writer(f_w, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    dept_writer.writerow(["id", "building_id", "name", "description", "parent_id"])
                    skip_header = next(dept_reader)
                    i = 1
                    for row in dept_reader:
                        if random.random() < 0.2:
                            choice = random.randint(1,3)

                            # Change building_id
                            if choice == 1:  
                                row[1] = random.randint(1,2)
                            #Change description
                            elif choice == 2:
                                row[3] = gen.sentence()
                            # Change parent_id:
                            elif choice == 3 and not i in [1,2, row[4]]:            
                                row[4] = random.randint(1,3)
                        dept_writer.writerow(row)
                        i += 1
        except FileNotFoundError:
            print("Incorrect file name!")

