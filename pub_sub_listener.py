import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")

topic_name = f'projects/{project_id}/topics/{topic_id}'

subscription_name = f'projects/{project_id}/subscriptions/{topic_id}-sub'


def callback(message):
    print(message.data)
    message.ack()

with pubsub_v1.SubscriberClient() as subscriber:
    try:
        subscriber.create_subscription(
            name=subscription_name, topic=topic_name)
    except Exception:
        pass
    future = subscriber.subscribe(subscription_name, callback)
    future.result()