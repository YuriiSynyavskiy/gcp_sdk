import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client()

dataset_id = os.environ.get("DATASET_ID")
project_id = os.environ.get("PROJECT_ID")

# Check if dataset is already existing

query = f"""
        SELECT schema_name
        FROM {project_id}.INFORMATION_SCHEMA.SCHEMATA;
"""

query_job = client.query(query)  

datasets = [dataset.schema_name for dataset in query_job]

if dataset_id in datasets:
    print("Dataset is already existing.")
else:
    # Create if dataset isn't existing
    query = f"""
        CREATE SCHEMA {dataset_id};
    """
    query_job = client.query(query)

    print(f"Dataset {dataset_id} was successfully created.")

# Passage Events Table
time.sleep(5)

query = f"""
        SELECT table_name
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
"""
query_job = client.query(query) 

tables = [table.table_name for table in query_job]

passage_table = 'fk_passage'

if passage_table in tables:
    print("Table fk_passage is already existing")
else:
    query = f"""
        CREATE TABLE {dataset_id}.{passage_table}(
            gate_id int not null,
            passcard_id int not null,
            status_id int not null,
            direction_id int not null, 
            timestamp timestamp not null
        );
    """
    query_job = client.query(query) 
    print(f"Table {passage_table} was successfully created.")
