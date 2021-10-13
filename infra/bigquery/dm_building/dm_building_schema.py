import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery


def create_dm_building_schema():
    load_dotenv()

    client = bigquery.Client()

    dataset_id = os.environ.get("DATASET_ID")
    project_id = os.environ.get("PROJECT_ID")

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
        query = """
        INSERT INTO {dataset_id}.{table_name} values 
            (1, 10, 'US', 'Oregon', 'Portland', 'Heron Way', '50B'),
            (2, 7, 'US', 'Pennsylvania', 'ANALOMINK', 'Bloomfield Way', '780'),
            (3, 5, 'US', 'Missouri', 'Saint Louis', 'Court Street', '4513'),
            (4, 20, 'US', 'Florida', 'Tampa', 'Monroe Avenue', '20'),
            (5, 14, 'US', 'Kansas', 'GOESSEL', 'Smithfield Avenue', '550'),
            (6, 10, 'US', 'Louisiana', 'Alexandria', 'Norma Lane', '4392'),
            (7, 30, 'US', 'Florida', 'Miami', 'Jarvis Street', '224'),
            (8, 8, 'US', 'Florida', 'Melbourne', 'Rosemont Avenue', '14'),
            (9, 5, 'US', 'Oklahoma', 'Texhoma', 'Grove Avenue', '2862'),
            (10, 16, 'US', 'Texas', 'San Angelo', 'Felosa Drive', '2560'),
            (11, 12, 'US', 'Illinois', 'Danville', 'Isaacs Creek Road', '3886'),
            (12, 6, 'US', '	North Carolina', 'Greenville', 'Green Acres Road', '670'),
            (13, 8, 'US', 'North Carolina', 'Winston Salem', 'Jones Avenue', '1006'),
            (14, 9, 'US', 'Texas', 'Corpus Christi', 'Franklin Avenue', '400'),
            (15, 20, 'US', 'Massachusetts', 'Bedford', 'Hampton Meadows', '345C'),
            (16, 13, 'US', 'Connecticut', 'Hartford', 'Maxwell Street', '467'),
            (17, 6, 'US', 'Arkansas', 'WALCOTT', 'State Street', '100'),
            (18, 10, 'US', 'Pennsylvania', 'Pittsburgh', 'Lucky Duck Drive', '12A'),
            (19, 40, 'US', 'New York', 'Floral Park', 'Gnatty Creek Road', '515'),
            (20, 15, 'US', 'Texas', 'Kerens', 'Pickens Way', '643');
        """
        query_job = client.query(query)
        print(f"Dimension {table_name} was successfully generated.")


if __name__ == "__main__":
    create_dm_building_schema()
