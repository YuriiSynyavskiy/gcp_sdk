import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_dm_status_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

    table_name = 'cl_status'

    query = f"""
            SELECT table_name
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES;
    """
    query_job = client.query(query) 

    tables = [table.table_name for table in query_job]

    if table_name in tables:
        print(f"Table {table_name} is already existing")
    else:
        query = f"""
            CREATE TABLE {dataset_id}.{table_name}(
                id INT,
                status_code INT,
                description STRING,
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")
        
        time.sleep(5)

        query = f"""
        INSERT {dataset_id}.{table_name} values 
            (1, 200, 'Successfuly'), 
            (2, 203, 'Successfuly(biometric failed)'),
            (3, 401, 'Access Denied'),
            (4, 402, 'Biometric failed(card ok)'),
            (5, 404, 'Unknown Error')
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")

if __name__ == "__main__":
    create_dm_status_schema()