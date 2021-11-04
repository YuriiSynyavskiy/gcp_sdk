import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client()

dataset_id = os.environ.get("DATASET_ID")

landing_dataset_id = os.environ.get("LANDING_DATASET_ID")

staging_dataset_id = os.environ.get("STAGING_DATASET_ID")
# Passage Events Table

query = f"""
        SELECT table_name
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

passage_table = 'fk_passage'

if passage_table in tables:
    print(f"Table {dataset_id}.{passage_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{passage_table}(
            id string not null,
            dm_gate_key int not null,
            dm_gate_id string not null,
            dm_passcard_key int not null,
            dm_passcard_id string not null,
            dm_status_id int not null,
            dm_direction_id int not null, 
            timestamp timestamp not null,
            dm_date_id int not null,
            dm_time_id string not null
        );
    """
    query_job = client.query(query) 
    print(f"Table {passage_table} in {dataset_id} was successfully created.")

query = f"""
        SELECT table_name
        FROM {landing_dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

passage_table = 'fk_passage'

if passage_table in tables:
    print(f"Table {landing_dataset_id}.{passage_table} is already existing")
else:
    query = f"""
        CREATE TABLE {landing_dataset_id}.{passage_table}(
            id string,
            dm_gate_key int,
            dm_passcard_key int,
            dm_status_id int,
            dm_direction_id int, 
            timestamp timestamp
        );
    """
    query_job = client.query(query) 
    print(f"Table {passage_table} in {landing_dataset_id} was successfully created.")


query = f"""
        SELECT table_name
        FROM {staging_dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

passage_table = 'fk_passage'

if passage_table in tables:
    print(f"Table {staging_dataset_id}.{passage_table} is already existing")
else:
    query = f"""
        CREATE TABLE {staging_dataset_id}.{passage_table}(
            id string,
            dm_gate_key int,
            dm_gate_id string,
            dm_passcard_key int,
            dm_passcard_id string,
            dm_status_id int,
            dm_direction_id int, 
            timestamp timestamp,
            dm_date_id int,
            dm_time_id string
        );
    """
    query_job = client.query(query) 
    print(f"Table {passage_table} in {staging_dataset_id} was successfully created.")


error_passage_table = 'error_fk_passage'

query = f"""
        SELECT table_name
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]



if error_passage_table in tables:
    print(f"Table {dataset_id}.{error_passage_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{error_passage_table}(
            id string,
            dm_gate_key int,
            dm_passcard_key int,
            dm_status_id int,
            dm_direction_id int, 
            timestamp timestamp
        );
    """
    query_job = client.query(query) 
    print(f"Table {error_passage_table} in {dataset_id} was successfully created.")