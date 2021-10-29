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


def create_dm_security_schema(dataset_id, table_name):
    """
        Create security schema
        :param: dataset_id
        :param: table_name
    """
    tables = check_table_exists(dataset_id)
    if table_name in tables:
        print(f"Table {table_name} already exists")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{table_name}(
                id STRING not null,
                parent_id STRING not null,
                name STRING,
                description STRING,
                biometric_verification STRING
            );
        """
        client.query(query) 
        print(f"Table {table_name} was successfully created in {dataset_id} schema.")

        time.sleep(5)

        query = f"""
        INSERT INTO {dataset_id}.{table_name}  
        VALUES('1', '1', ' security_1', 'SEO', 'TRUE')
        VALUES('2', '2', ' security_2', 'office management', 'TRUE')
        VALUES('3', '3', ' security_3', 'workers', 'FALSE');
        """
        client.query(query)
        print(f"Dimension {table_name} was successfully generated.")


if __name__ == "__main__":
    load_dotenv()

    client = bigquery.Client()
    dataset_id = os.environ.get("DATASET_ID")

    table_name = "dm_security"
    create_dm_security_schema(dataset_id, table_name)
