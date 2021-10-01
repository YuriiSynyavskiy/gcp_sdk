import os
from google.cloud import storage
from dotenv import load_dotenv
from google.api_core.exceptions import Conflict

load_dotenv()

bucket_id = os.environ.get("BUCKET_ID")

storage_client = storage.Client()

bucket = storage_client.bucket(bucket_id)
bucket.storage_class = "STANDARD"

try:
    new_bucket = storage_client.create_bucket(bucket, location="eu")


    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    blob = new_bucket.blob('tmp/')
    blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
    
    blob = new_bucket.blob('staging/')
    blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
    
    print("Subdirectories created.")
except Conflict:
    print("Bucket with such name is already created.")
