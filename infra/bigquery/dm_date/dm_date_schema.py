import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

def create_dm_date_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")

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
                id INT,
                year INT,
                month INT,
                day INT,
                quarter INT,
                month_name STRING,
                week_day INT,
                day_name STRING,
                day_is_weekday INT
            );
        """
        query_job = client.query(query) 
        print(f"Table {table_name} was successfully created.")

        time.sleep(5)
        query = f"""
        INSERT INTO {dataset_id}.{table_name} 
        SELECT 
            CAST(REPLACE(FORMAT_DATE('%F', d), '-', '') as INT) as id, 
            EXTRACT(YEAR FROM d) AS year, 
            EXTRACT(MONTH FROM d) AS month, 
            EXTRACT(DAY FROM d) AS day,
            EXTRACT(QUARTER FROM d) AS quarter,
            FORMAT_DATE('%B', d) as month_name,
            CAST(FORMAT_DATE('%w', d) as INT) AS week_day,
            FORMAT_DATE('%A', d) AS day_name,
            (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS day_is_weekday 
        FROM ( 
            SELECT * 
            FROM 
            UNNEST(GENERATE_DATE_ARRAY('1899-01-01', '2999-01-01', INTERVAL 1 DAY)) AS d
            );
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")

if __name__ == "__main__":
    create_dm_date_schema()