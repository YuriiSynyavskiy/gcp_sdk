import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_throughput_datamart():
    load_dotenv()

    client = bigquery.Client()

    datamart_dataset_id = os.environ.get("DATAMART_DATASET_ID")

    table_name = 'throughput_datamart'

    query = f"""
            SELECT table_name
            FROM {datamart_dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    tables = [table.table_name for table in query_job]

    if table_name in tables:
        print(f"Table {table_name} is already existing")
    else:
        query = f"""
            CREATE TABLE {datamart_dataset_id}.{table_name}(
                dm_gate_id STRING,
                gate_key STRING,
                timestamp TIMESTAMP,
                throughput INT
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")


if __name__ == "__main__":
    create_throughput_datamart()