import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client()

dataset_id = os.environ.get("DATASET_ID")

# Passage Events Table

query = f"""
        SELECT table_name
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

passage_table = 'fk_passage'

if passage_table in tables:
    print(f"Table {passage_table} is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{passage_table}(
            id string not null,
            dm_gate_id string not null,
            dm_gate_key int not null,
            dm_passcard_id string not null,
            dm_passcard_key int not null,
            dm_status_id int not null,
            dm_direction_id int not null, 
            timestamp timestamp not null,
            dm_date_id int not null,
            dm_time_id string not null
        );
    """
    query_job = client.query(query) 
    print(f"Table {passage_table} was successfully created.")
