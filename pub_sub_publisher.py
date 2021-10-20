import os
import json
import time
import uuid
from datetime import date, datetime
from random import randint
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")

publisher = pubsub_v1.PublisherClient()

topic_name = f'projects/{project_id}/topics/{topic_id}'



while True:
    now =  datetime.now().utcnow()
    message = {
        "id": str(uuid.uuid4()),
        "dm_gate_id": randint(1,10),
        "dm_passcard_id": randint(1,10),
        "dm_status_id": randint(1,4),
        "dm_direction_id": randint(1,2),
        "timestamp": f"{str(now.utcnow())} UTC",
        "dm_date_id": int(now.strftime('%Y%m%d')),
        "dm_time_id": now.strftime('%H%M%S')
        }

    future = publisher.publish(topic_name, json.dumps(message).encode("utf-8"))
    future.result()
    time.sleep(5)
