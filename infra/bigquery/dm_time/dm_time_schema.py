import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_dm_time_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

    table_name = 'dm_time'

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
                id STRING,
                hours INT,
                minutes INT,
                seconds INT
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")
        
        time.sleep(5)

        query = """
        INSERT INTO {dataset_id}.{table_name}  
        SELECT FORMAT_TIMESTAMP("%H-%M-%S", TIMESTAMP_SECONDS(d)) as id, 
               EXTRACT(hour from TIMESTAMP_SECONDS(d)) as hours, 
               EXTRACT(minute from TIMESTAMP_SECONDS(d)) as minutes, 
               EXTRACT(second from TIMESTAMP_SECONDS(d)) as seconds, 
        FROM UNNEST(generate_array(1609459200,1609545599)) AS d;
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")

if __name__ == "__main__":
    create_dm_time_schema()