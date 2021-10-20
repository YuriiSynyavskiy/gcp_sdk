from datetime import date
import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_person_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")

    landing_dataset_id = os.environ.get("LANDING_DATASET_ID")
    staging_dataset_id = os.environ.get("STAGING_DATASET_ID")
    # Creating Person models for SCD

    person_table_name = "dm_person"

    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    target_tables = [table.table_name for table in query_job]

    if person_table_name in target_tables:
        print(f"Table {person_table_name} is already existing in {dataset_id}")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{person_table_name}(
                dm_person_id STRING not null,
                person_key STRING not null,
                current_dm_department_id STRING,
                prev_dm_department_id STRING,
                dm_position_id STRING,
                name STRING,
                surname STRING,
                salary INT,
                phone STRING,
                start_date DATE,
                start_date_id INT,
                end_date DATE,
                end_date_id INT,
                effective_start_date DATETIME,
                effective_end_date DATETIME,
                current_flag STRING,
                hash_key STRING not null
            );
        """
        query_job = client.query(query) 
        print(f"Table {person_table_name} was successfully created in {dataset_id}.")

    query = f"""
            SELECT table_name
            FROM {landing_dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    landing_tables = [table.table_name for table in query_job]
    tmp_person_table_name = f"tmp_{person_table_name}"

    if tmp_person_table_name in landing_tables:
        print(f"Table {tmp_person_table_name} is already existing in {landing_dataset_id}")
    else:
        query = f"""
            CREATE TABLE {landing_dataset_id}.{tmp_person_table_name}(
                person_key STRING not null,
                department_id STRING,
                position_id STRING,
                name STRING,
                surname STRING,
                salary INT,
                phone STRING,
                start_date DATE,
                end_date DATE,
            );
        """
        query_job = client.query(query)
        print(f"Table {tmp_person_table_name} was successfully created in {landing_dataset_id}.")

    if person_table_name in landing_tables:
        print(f"Table {person_table_name} is already existing in {landing_dataset_id}")
    else:
        query = f"""
            CREATE TABLE {landing_dataset_id}.{person_table_name}(
                run_id STRING not null,
                person_key STRING not null,
                department_id STRING,
                position_id STRING,
                name STRING,
                surname STRING,
                salary INT,
                phone STRING,
                start_date DATE,
                end_date DATE,
            );
        """
        query_job = client.query(query)
        print(f"Table {person_table_name} was successfully created in {landing_dataset_id}.")

    query = f"""
            SELECT table_name
            FROM {staging_dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    staging_tables = [table.table_name for table in query_job]


    if person_table_name in staging_tables:
        print(f"Table {person_table_name} is already existing in {staging_dataset_id}")
    else:
        query = f"""
            CREATE TABLE {staging_dataset_id}.{person_table_name}(
                person_key STRING not null,
                department_id STRING,
                position_id STRING,
                name STRING,
                surname STRING,
                salary INT,
                phone STRING,
                start_date DATE,
                end_date DATE,
                hash_key STRING not null
            );
        """
        query_job = client.query(query)
        print(f"Table {person_table_name} was successfully created in {staging_dataset_id}.")

if __name__ == "__main__":
    create_person_schema()