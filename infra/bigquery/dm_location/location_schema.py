import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery


def check_table_exists(dataset_id):
    """
        Check if table exist in dataset
    """
    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query)
    tables = [table.table_name for table in query_job]

    return tables


def create_landing_schema(dataset_id, table_name):
    """
        Create landing schema
        :param: dataset_id
        :param: table_name
    """
    tables = check_table_exists(dataset_id)
    print(tables)
    if table_name in tables:
        print(f"Table {table_name} already exists")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{table_name}(
                run_id STRING,
                location_key STRING not null,
                building_id STRING,
                security_id STRING,
                gate_id STRING,
                room_number INT,
                floor INT,
                description STRING,
            );
        """
        print(query)
        client.query(query) 
        print(f"Table {table_name} was successfully created in {dataset_id} schema.")


def create_staging_schema(dataset_id, table_name):
    """
        Create staging schema
        :param: dataset_id
        :param: table_name
    """
    tables = check_table_exists(dataset_id)
    if table_name in tables:
        print(f"Table {table_name} already exists")
    else:
        query = f"""
                CREATE TABLE {dataset_id}.{table_name}(
                    location_key STRING not null,
                    building_id STRING,
                    security_id STRING,
                    gate_id STRING,
                    room_number INT,
                    floor INT,
                    description STRING,
                    hash_id STRING not null
                );
            """
        client.query(query)
        print(f"Table {table_name} was successfully created in {dataset_id} schema.")


def create_target_schema(dataset_id, table_name):
    """
        Create target schema
        :param: dataset_id
        :param: table_name
    """
    tables = check_table_exists(dataset_id)
    if table_name in tables:
        print(f"Table {table_name} already exists")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{table_name}(
                dm_location_id STRING not null,
                location_key STRING not null,
                dm_building_id STRING,
                dm_security_id STRING,
                dm_gate_id STRING,
                room_number INT,
                floor INT,
                description STRING,
                effective_start_date DATETIME,
                effective_end_date DATETIME,
                flag STRING,
                hash_id STRING not null
            );
        """
        client.query(query) 
        print(f"Table {table_name} was successfully created in {dataset_id} schema.")


if __name__ == "__main__":
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    landing_dataset_id = os.environ.get("LANDING_DATASET_ID")
    staging_dataset_id = os.environ.get("STAGING_DATASET_ID")

    table_name = "dm_location"

    create_landing_schema(landing_dataset_id, table_name)
    create_staging_schema(staging_dataset_id, table_name)
    create_target_schema(dataset_id, table_name)
