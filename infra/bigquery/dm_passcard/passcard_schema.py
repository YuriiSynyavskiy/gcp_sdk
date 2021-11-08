import os
from typing import List

from dotenv import load_dotenv
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import SchemaField, Table

load_dotenv()


def create_table_in_dataset(
    bq_client: BigQueryClient,
    project_id,
    dataset_id: str,
    table_name: str,
    schema: List[SchemaField],
    exists_ok=True,
) -> Table:
    table = Table('.'.join([
        project_id,
        dataset_id,
        table_name,
    ]), schema=schema)

    table = bq_client.create_table(table, exists_ok=exists_ok)

    return table.full_table_id


def init():
    passcard_table_name = 'dm_passcard'

    staging_schema = [
        SchemaField('id', 'INTEGER', mode='REQUIRED'),
        SchemaField('person_id', 'STRING', mode='REQUIRED'),
        SchemaField('security_id', 'INTEGER', mode='REQUIRED'),
        SchemaField('start_date', 'INTEGER', mode='REQUIRED'),
        SchemaField('expires_at', 'INTEGER'),
        SchemaField('hash', 'STRING', mode='REQUIRED'),
    ]
    target_schema = [
        SchemaField('passcard_key', 'INTEGER', mode='REQUIRED'),
        SchemaField('dm_passcard_id', 'STRING', mode='REQUIRED'),
        SchemaField('dm_person_id', 'STRING', mode='REQUIRED'),
        SchemaField('dm_security_id', 'INTEGER', mode='REQUIRED'),
        SchemaField('start_date', 'INTEGER', mode='REQUIRED'),
        SchemaField('expires_at', 'INTEGER', mode='REQUIRED'),
        SchemaField('effective_start_date', 'DATETIME', mode='NULLABLE'),
        SchemaField('effective_end_date', 'DATETIME', mode='NULLABLE'),
        SchemaField('current_flag', 'STRING', mode='REQUIRED'),
        SchemaField('hash', 'STRING', mode='REQUIRED'),
    ]

    client = BigQueryClient()

    table = create_table_in_dataset(
        client,
        os.environ.get('PROJECT_ID'),
        os.environ.get('DATASET_STAGING_ID'),
        passcard_table_name,
        staging_schema,
        True,
    )
    print(f'Created table `{table.full_table_id}`')

    table = create_table_in_dataset(
        client,
        os.environ.get('PROJECT_ID'),
        os.environ.get('DATASET_TARGET_ID'),
        passcard_table_name,
        target_schema,
        True,
    )
    print(f'Created table `{table.full_table_id}`')


if __name__ == '__main__':
    init()
