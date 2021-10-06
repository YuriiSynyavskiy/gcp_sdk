import os
import argparse
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
parser = argparse.ArgumentParser(description='Upload file')
parser.add_argument('--bucket_path', type=str, help='Bucket path', default=f"{os.environ.get('AIRFLOW_BUCKET_ID')}/dags")
parser.add_argument('--file_path', type=str, help='Path for file to upload', required=True)

args = parser.parse_args()

bucket_parts = args.bucket_path.split("/")

storage_client = storage.Client()

bucket = storage_client.get_bucket(bucket_parts[0])

path = [x for x in bucket_parts[1:] if x]
blob_path = f"{'/'.join(path)}/{args.file_path.split('/')[-1]}"

blob = bucket.blob(blob_path)
blob.upload_from_filename(args.file_path)

print(f"File {args.file_path.split('/')[-1]} successfully loaded.")