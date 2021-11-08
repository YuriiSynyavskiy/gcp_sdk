import os
import random
from collections import namedtuple
from typing import List

from dotenv import load_dotenv
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import SchemaField, Table

load_dotenv()

Building = namedtuple(
    'Building',
    [
        'id', 'storey', 'country', 'state',
        'city', 'street', 'building_number',
    ],
)


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

    return table


def generate_fake_buildings(fake, n: int) -> List[Building]:
    buildings = []

    for i in range(1, n):
        buildings.append(
            Building(
                i,
                random.randint(1, 101),
                fake.country(),
                fake.state(),
                fake.city(),
                fake.street_name(),
                fake.building_number(),
            )
        )

    return buildings


def init():
    building_table_name = 'dm_building'

    schema = [
        SchemaField('id', 'INTEGER', mode='REQUIRED'),
        SchemaField('storey', 'INTEGER', mode='REQUIRED'),
        SchemaField('country', 'STRING', mode='REQUIRED'),
        SchemaField('state', 'STRING', mode='REQUIRED'),
        SchemaField('city', 'STRING', mode='REQUIRED'),
        SchemaField('street', 'STRING', mode='REQUIRED'),
        SchemaField('building_number', 'STRING', mode='REQUIRED'),
    ]

    client = BigQueryClient()

    table = create_table_in_dataset(
        client,
        os.environ.get('PROJECT_ID'),
        os.environ.get('DATASET_ID'),
        building_table_name,
        schema,
        True,
    )
    print(f'Created table `{table.full_table_id}`')

    try:
        from faker import Faker
    except ImportError:
        print('To generate data install `faker` the package')

        return

    fake = Faker()

    buildings = generate_fake_buildings(fake, 101)

    client.insert_rows(table, buildings)


if __name__ == '__main__':
    init()
