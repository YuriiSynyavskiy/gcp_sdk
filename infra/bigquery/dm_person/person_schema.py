import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client()

dataset_id = os.environ.get("DATASET_ID")
project_id = os.environ.get("PROJECT_ID")

# Creating Person models for SCD

target_person_table = "dm_person"

l_person_table = f"landing_{target_person_table}"

stg_person_table = f"staging_{target_person_table}"

query = f"""
        SELECT table_name
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

if target_person_table in tables:
    print(f"Table {target_person_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{target_person_table}(
            hash_id STRING not null,
            dm_person_id STRING not null,
            person_id STRING not null,
            current_dm_department_id STRING,
            prev_dm_department_id STRING,
            dm_position_id STRING,
            name STRING,
            surname STRING,
            salary INT,
            phone STRING,
            start_date DATETIME,
            end_date DATETIME,
            effective_start_date DATETIME,
            effective_end_date DATETIME,
            current_flag STRING 
        );
    """
    query_job = client.query(query) 
    print(f"Table {target_person_table} was successfully created.")

if l_person_table in tables:
        print(f"Table {l_person_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{l_person_table}(
            person_id STRING not null,
            department_id STRING,
            position_id STRING,
            name STRING,
            surname STRING,
            salary INT,
            phone STRING,
            start_date DATETIME,
            end_date DATETIME,
        );
    """
    query_job = client.query(query)
    print(f"Table {l_person_table} was successfully created.")


if stg_person_table in tables:
        print(f"Table {stg_person_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{stg_person_table}(
            hash_id STRING not null,
            person_id STRING not null,
            department_id STRING,
            position_id STRING,
            name STRING,
            surname STRING,
            salary INT,
            phone STRING,
            start_date DATETIME,
            end_date DATETIME,
        );
    """
    query_job = client.query(query)
    print(f"Table {stg_person_table} was successfully created.")