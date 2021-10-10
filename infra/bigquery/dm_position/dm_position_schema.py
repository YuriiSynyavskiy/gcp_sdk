import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_dm_position_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

    table_name = 'dm_position'

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
                name STRING
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")
        
        time.sleep(5)

        query = """
        INSERT {dataset_id}.{table_name} values 
            (1, 'Developer'),
            (2, 'QA'),
            (3, 'Architect'),
            (4, 'Project Manager'),
            (5, 'Englist Teacher'),
            (6, 'Salesforce'),
            (7, 'DevOps'),
            (8, 'AQA'),
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")

if __name__ == "__main__":
    create_dm_position_schema()