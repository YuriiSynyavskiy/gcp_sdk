from dotenv import dotenv_values
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import SchemaField, Table
from google.oauth2 import service_account

config = dotenv_values('.env')


def get_bq_client() -> BigQueryClient:
    credentials = service_account.Credentials.from_service_account_file(
        config.get('GOOGLE_APPLICATION_CREDENTIALS'),
    )

    return BigQueryClient(credentials=credentials)


def create_passcard_landing_table():
    client = get_bq_client()

    schema = [
        SchemaField('passcard_key', 'STRING', mode='REQUIRED'),
        SchemaField('person_id', 'INTEGER', mode='REQUIRED'),
        SchemaField('security_id', 'INTEGER', mode='REQUIRED'),
        SchemaField('start_date', 'DATE', mode='REQUIRED'),
        SchemaField('expires_at', 'DATE'),
    ]

    table = Table('.'.join([
        config.get('PROJECT_ID'),
        config.get('DATASET_LANDING_ID'),
        'dm_passcard',
    ]), schema=schema)

    table = client.create_table(table)

    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )


if __name__ == '__main__':
    create_passcard_landing_table()
