import os
from dotenv import load_dotenv
from google.pubsub_v1.types import Schema
from google.pubsub_v1.types import Encoding
from google.cloud.pubsub import PublisherClient, SchemaServiceClient
from google.api_core.exceptions import AlreadyExists


load_dotenv()

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")
schema_id = os.environ.get("SCHEMA_ID")
schema_avsc_path = "./schema.avsc"
project_path = f"projects/{project_id}"

# Creating of schema

schema_client = SchemaServiceClient()
schema_path = schema_client.schema_path(project_id, schema_id)




with open(schema_avsc_path, "rb") as f:
    proto_source = f.read().decode("utf-8")
schema = Schema(
    name=schema_path, type_=Schema.Type.AVRO, definition=proto_source
)
try:
    result = schema_client.create_schema(
        request={"parent": project_path, "schema": schema, "schema_id": schema_id}
    )
    print(f"Created a schema using a AVRO schema file:\n{result}") # logging
except AlreadyExists:
    print(f"{schema_id} already exists.") # logging

schema_encoding = Encoding.JSON
publisher_client = PublisherClient()

topic_path = publisher_client.topic_path(project_id, topic_id)
try:
    response = publisher_client.create_topic(
        request={
            "name": topic_path,
            "schema_settings": {"schema": schema_path, "encoding": schema_encoding},
        }
    )
    print(f"Created a topic:\n{response}")
except AlreadyExists:
    print(f"{topic_id} already exists.")