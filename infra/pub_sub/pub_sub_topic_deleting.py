import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import SchemaServiceClient


load_dotenv()
project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")
schema_id = os.environ.get("SCHEMA_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

publisher.delete_topic(request={"topic": topic_path})
print(f"Topic deleted: {topic_path}")


schema_client = SchemaServiceClient()
schema_path = schema_client.schema_path(os.environ.get("PROJECT_ID"), os.environ.get("SCHEMA_ID"))


try:
    schema_client.delete_schema(request={"name": schema_path})
    print(f"Deleted a schema: {schema_path}")
except NotFound:
    print(f"{schema_id} not found.")