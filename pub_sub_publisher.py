import os
import json
import time
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
    message = {
        "gate_id":str(randint(1,10)),
        "passcard_id":str(randint(1,10)),
        "status_id":randint(1,4),
        "direction_id":randint(1,2),
        "timestamp": str(datetime.timestamp(datetime.now())).split('.')[0]
        }


    future = publisher.publish(topic_name, json.dumps(message).encode("utf-8"))
    future.result()
    time.sleep(5)
