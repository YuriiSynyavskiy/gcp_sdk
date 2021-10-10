import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_dm_date_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

    table_name = 'dm_date'

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
                day INT,
                month INT,
                year INT,
                quarter INT
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")

        time.sleep(5)
        query = """
        INSERT INTO {dataset_id}.{table_name} 
        SELECT 
            FORMAT_DATE('%F', d) as id, 
            EXTRACT(YEAR FROM d) AS year, 
            EXTRACT(DAY FROM d) AS day,
            EXTRACT(MONTH FROM d) AS month, 
            EXTRACT(QUARTER FROM d) AS quarter, 
        FROM ( 
            SELECT * 
            FROM 
            UNNEST(GENERATE_DATE_ARRAY('1899-01-01', '3999-01-01', INTERVAL 1 DAY)) AS d
            );
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")

if __name__ == "__main__":
    create_dm_date_schema()