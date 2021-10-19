import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import table


def create_dm_department_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    landing_dataset_id = os.environ.get("LANDING_DATASET_ID")
    staging_dataset_id = os.environ.get("STAGING_DATASET_ID")

    
    # Creating Department's models for SCD
    table_name = "dm_department"

    query = f"""
            SELECT table_name
            FROM {landing_dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query)

    landing_tables = [table.table_name for table in query_job]
    tmp_table_name = f'tmp_{table_name}'

    if tmp_table_name in landing_tables:
        print(f"Table {tmp_table_name} is already existing in {landing_dataset_id} schema.")
    else:
        query = f"""
            CREATE TABLE {landing_dataset_id}.{tmp_table_name}(
                department_key STRING,
                building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING
            );
        """
        query_job = client.query(query)
        print(f"Table {tmp_table_name} was successfully created in {landing_dataset_id} schema.")
    
    query = f"""
            SELECT table_name
            FROM {staging_dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query)

    staging_tables = [table.table_name for table in query_job]

    if table_name in staging_tables:
        print(f"Table {table_name} is already existing in {staging_dataset_id} schema.")
    else:
        query = f"""
            CREATE TABLE {staging_dataset_id}.{table_name}(
                department_key STRING not null,
                building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING,
                hash_key STRING not null,
            );
        """
        query_job = client.query(query)
        print(f"Table {table_name} was successfully created in {staging_dataset_id} schema.")

    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query)

    target_tables = [table.table_name for table in query_job]

    if table_name in target_tables:
        print(f"Table {table_name} is already existing in {dataset_id} schema.")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{table_name}(
                dm_department_id STRING not null,
                department_key STRING not null,
                dm_building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING,
                effective_start_date DATETIME,
                effective_end_date DATETIME,
                current_flag STRING,
                hash_key STRING not null,
            );
        """
        query_job = client.query(query)
        print(f"Table {table_name} was successfully created in {dataset_id} schema.")


if __name__ == "__main__":
    create_dm_department_schema()
