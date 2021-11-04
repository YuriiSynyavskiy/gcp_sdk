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
        "dm_gate_key": randint(-1,2),
        "dm_passcard_key": randint(0, 10),
        "dm_status_id": randint(1,4),
        "dm_direction_id": randint(1,2),
        "timestamp": f"{str(now.utcnow())} UTC",
        }
    
    client = bigquery.Client()

    future = publisher.publish(topic_name, json.dumps(message).encode("utf-8"))
    future.result()
    time.sleep(3)
