import os
import time
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client()

dataset_id = os.environ.get("DATASET_ID")
staging_dataset_id = os.environ.get("STAGING_DATASET_ID")
landing_dataset_id = os.environ.get("LANDING_DATASET_ID")
project_id = os.environ.get("PROJECT_ID")
datamarts_dataset_id = os.environ.get("DATAMART_DATASET_ID")
# Check if dataset is already existing

query = f"""
        SELECT schema_name
        FROM {project_id}.INFORMATION_SCHEMA.SCHEMATA;
"""

query_job = client.query(query)  

datasets = [dataset.schema_name for dataset in query_job]

if dataset_id in datasets:
    print(f"Dataset {dataset_id} is already existing.")
else:
    # Create if dataset isn't existing
    query = f"""
        CREATE SCHEMA {dataset_id};
    """
    query_job = client.query(query)

    print(f"Dataset {dataset_id} was successfully created.")


if staging_dataset_id in datasets:
    print(f"Dataset {staging_dataset_id} is already existing.")
else:
    # Create if dataset isn't existing
    query = f"""
        CREATE SCHEMA {staging_dataset_id};
    """
    query_job = client.query(query)

    print(f"Dataset {staging_dataset_id} was successfully created.")


if landing_dataset_id in datasets:
    print(f"Dataset {landing_dataset_id} is already existing.")
else:
    # Create if dataset isn't existing
    query = f"""
        CREATE SCHEMA {landing_dataset_id};
    """
    query_job = client.query(query)

    print(f"Dataset {landing_dataset_id} was successfully created.")


if datamarts_dataset_id in datasets:
    print(f"Dataset {datamarts_dataset_id} is already existing.")
else:
    # Create if dataset isn't existing
    query = f"""
        CREATE SCHEMA {datamarts_dataset_id};
    """
    query_job = client.query(query)

    print(f"Dataset {datamarts_dataset_id} was successfully created.")
