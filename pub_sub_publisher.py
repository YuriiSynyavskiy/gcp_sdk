import os
import json
import time
import uuid
from datetime import datetime
from random import randint
from google.cloud import pubsub_v1, bigquery
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")
dataset_id = os.environ.get("DATASET_ID")
publisher = pubsub_v1.PublisherClient()
passcard_table_name = "dm_passcard"
gate_table_name = 'dm_gate'
topic_name = f'projects/{project_id}/topics/{topic_id}'



while True:
    now =  datetime.now().utcnow()
    message = {
        "id": str(uuid.uuid4()),
        "dm_gate_key": randint(1,10),
        "dm_gate_id": "",
        "dm_passcard_key": randint(1,10),
        "dm_passcard_id": "",
        "dm_status_id": randint(1,4),
        "dm_direction_id": randint(1,2),
        "timestamp": f"{str(now.utcnow())} UTC",
        "dm_date_id": int(now.strftime('%Y%m%d')),
        "dm_time_id": now.strftime('%H%M%S')
        }
    
    client = bigquery.Client()
    # TODO Change {dm_passcard_key} -> {dm_passcard_id} and {id} -> {dm_passcard_key}
    query_job = client.query(
        f"""SELECT dm_passcard_key 
            FROM {dataset_id}.{passcard_table_name} 
            WHERE id = '{message['dm_passcard_key']}' and is_current = 'Y' """
    )

    result = next(query_job.result())
    message["dm_passcard_id"] = result.dm_passcard_key

    query_job = client.query(
        f"""SELECT dm_gate_id 
            FROM {dataset_id}.{gate_table_name} 
            WHERE gate_key = '{message['dm_gate_key']}' and flag = 'Y' """
    )

    result = next(query_job.result())
    message["dm_gate_id"] = result.dm_passcard_key

    future = publisher.publish(topic_name, json.dumps(message).encode("utf-8"))
    future.result()
    time.sleep(3)
