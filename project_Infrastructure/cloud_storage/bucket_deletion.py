import os
from google.cloud import storage
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound

load_dotenv()

bucket_id = os.environ.get("BUCKET_ID")

storage_client = storage.Client()
try:
    bucket = storage_client.get_bucket(bucket_id)
    bucket.delete(force=True)
    print("Bucket {} deleted".format(bucket.name))
except NotFound:
    print("The specified bucket does not exist.")
