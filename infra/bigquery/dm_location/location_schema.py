import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_location_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

    # Creating Location models for SCD

    target_location_table = "dm_location"

    l_location_table = f"landing_{target_location_table}"

    stg_location_table = f"staging_{target_location_table}"

    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    tables = [table.table_name for table in query_job]

    if target_location_table in tables:
        print(f"Table {target_location_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{target_location_table}(
                hash_id STRING not null,
                dm_location_id STRING not null,
                location_id STRING not null,
                dm_building_id STRING,
                dm_security_id STRING,
                dm_gate_id STRING,
                room_number INT,
                floor INT,
                description STRING,
                effective_start_date DATETIME,
                effective_end_date DATETIME,
                flag STRING 
            );
        """
        query_job = client.query(query) 
        print(f"Table {target_location_table} was successfully created.")

    if l_location_table in tables:
        print(f"Table {l_location_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{l_location_table}(
                location_id STRING not null,
                building_id STRING,
                security_id STRING,
                gate_id STRING,
                room_number INT,
                floor INT,
                description STRING,
            );
        """
        query_job = client.query(query)
        print(f"Table {l_location_table} was successfully created.")


    if stg_location_table in tables:
        print(f"Table {stg_location_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{stg_location_table}(
                hash_id STRING not null,
                location_id STRING not null,
                building_id STRING,
                security_id STRING,
                gate_id STRING,
                room_number INT,
                floor INT,
                description STRING,
            );
        """
        query_job = client.query(query)
        print(f"Table {stg_location_table} was successfully created.")

if __name__ == "__main__":
    create_location_schema()
