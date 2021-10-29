from typing import List

from dotenv import dotenv_values
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import Table, SchemaField
from google.oauth2 import service_account

config = dotenv_values('.env')

PASSCARD_TABLE_NAME = 'dm_passcard'


def get_bq_client() -> BigQueryClient:
    credentials = service_account.Credentials.from_service_account_file(
        config.get('GOOGLE_APPLICATION_CREDENTIALS'),
    )

    return BigQueryClient(credentials=credentials)


def create_passcard_table_in_dataset(
    bq_client: BigQueryClient,
    dataset_id: str,
    schema: List[SchemaField]
) -> None:
    table = Table('.'.join([
        config.get('PROJECT_ID'),
        dataset_id,
        PASSCARD_TABLE_NAME,
    ]), schema=schema)

    table = bq_client.create_table(table, exists_ok=True)
    print(f'Created table `{table.full_table_id}`')


def init():
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

    client = get_bq_client()

    create_passcard_table_in_dataset(
        client,
        config.get('DATASET_STAGING_ID'),
        staging_schema,
    )
    create_passcard_table_in_dataset(
        client,
        config.get('DATASET_TARGET_ID'),
        target_schema,
    )


if __name__ == '__main__':
    init()
