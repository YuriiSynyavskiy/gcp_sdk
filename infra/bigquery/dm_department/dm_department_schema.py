import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery


def create_dm_department_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    # Creating Department's models for SCD

    target_dept_table = "dm_department"

    l_dept_table = f"landing_{target_dept_table}"

    stg_dept_table = f"staging_{target_dept_table}"

    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query)

    tables = [table.table_name for table in query_job]

    if l_dept_table in tables:
        print(f"Table {l_dept_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{l_dept_table}(
                department_id STRING,
                building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING
            );
        """
        query_job = client.query(query)
        print(f"Table {l_dept_table} was successfully created.")

    if stg_dept_table in tables:
        print(f"Table {stg_dept_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{stg_dept_table}(
                hash_id STRING not null,
                department_id STRING not null,
                building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING
            );
        """
        query_job = client.query(query)
        print(f"Table {stg_dept_table} was successfully created.")

    if target_dept_table in tables:
        print(f"Table {target_dept_table} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{target_dept_table}(
                hash_id STRING not null,
                dm_department_id STRING not null,
                department_id STRING not null,
                dm_building_id STRING,
                name STRING,
                description STRING,
                parent_id STRING,
                effective_start_date DATETIME,
                effective_end_date DATETIME,
                current_flag STRING 
            );
        """
        query_job = client.query(query)
        print(f"Table {target_dept_table} was successfully created.")


if __name__ == "__main__":
    create_dm_department_schema()
