import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery


def create_dm_building_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")

    table_name = 'dm_building'

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
                storey INT,
                country STRING,
                state STRING,
                city STRING,
                street STRING,
                apartment_number STRING
            );
        """
        query_job = client.query(query)
        print(f"Table {table_name} was successfully created.")

        time.sleep(5)
        query = f"""
        INSERT INTO {dataset_id}.{table_name} values 
            (1, 10, 'US', 'Oregon', 'Portland', 'Heron Way', '50B'),
            (2, 7, 'US', 'Pennsylvania', 'ANALOMINK', 'Bloomfield Way', '780')
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")


if __name__ == "__main__":
    create_dm_building_schema()
