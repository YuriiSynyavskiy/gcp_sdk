import concurrent.futures
import json
import os
import time
import uuid
from datetime import datetime
from random import randint

import typer
from dotenv import load_dotenv
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.pubsub_v1 import PublisherClient

load_dotenv()

app = typer.Typer()


def get_request_to_bq(client: BigQueryClient, query: str) -> str:
    query_job = client.query(query)
    result = query_job.result().next()

    return result[0]


@app.command('gen_passages')
def gen_passages(number: int):
    bq_client = BigQueryClient()
    publisher = PublisherClient()

    project_id = os.environ.get('PROJECT_ID')
    topic_id = os.environ.get('TOPIC_ID')
    dataset_id = os.environ.get('DATASET_ID')

    passcard_table_name = 'dm_passcard'
    gate_table_name = 'dm_gate'

    topic_name = publisher.topic_path(project_id, topic_id)

    futures = []

    for _ in range(0, number):
        now = datetime.now().utcnow()

        message = {
            'id': str(uuid.uuid4()),
            'dm_gate_key': randint(1, 10),
            'dm_gate_id': '',
            'dm_passcard_key': randint(1, 100),
            'dm_passcard_id': '',
            'dm_status_id': randint(1, 4),
            'dm_direction_id': randint(1, 2),
            'timestamp': f'{str(now.utcnow())} UTC',
            'dm_date_id': int(now.strftime('%Y%m%d')),
            'dm_time_id': now.strftime('%H%M%S')
        }

        passcard_query = f'''
            SELECT dm_passcard_id
            FROM `{dataset_id}.{passcard_table_name}`
            WHERE passcard_key = {message['dm_passcard_key']}
            AND current_flag = "Y"
        '''
        gate_query = f'''
            SELECT dm_gate_id
            FROM `{dataset_id}.{gate_table_name}`
            WHERE gate_key = "{message['dm_gate_key']}"
            AND flag = "Y"
        '''

        message['dm_passcard_id'] = get_request_to_bq(
            bq_client, passcard_query,
        )
        message['dm_gate_id'] = get_request_to_bq(
            bq_client, gate_query,
        )

        future = publisher.publish(
            topic_name,
            json.dumps(message).encode('utf-8'),
        )
        future.add_done_callback(
            lambda _: print(f'Card {message["dm_passcard_key"]} '
                            f'passed through gate {message["dm_gate_key"]} '
                            f'at {now}'),
        )

        time.sleep(randint(1, 5))

    concurrent.futures.wait(futures)


if __name__ == '__main__':
    app()

