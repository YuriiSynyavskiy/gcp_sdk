# gcp_sdk

To run queries:

Go to IAM&Admin -> Service accounts, choose existing service or generate new with your roles.

Click Actions -> Manage keys -> Add key -> Put google-credentials.json to ~/.

In terminal define path to your file:

export GOOGLE_APPLICATION_CREDENTIALS="/home/<your-user>/<file-name>.json"

You need generate file .env where must be structure:

PROJECT_ID=<project-id>

TOPIC_ID=<topic-name>

SCHEMA_ID=<schema-name>

DATASET_ID=<dataset-name>

Generate topic in Pub Sub with script - pub_sub_topic_creation.py.

Generate schema of BigQuery with script - bigquery_schema.py


To generate some number of passages run `python ./generate_passages.py NUM`

!!!IMPORTANT!!!

If you want to run pub_sub_topic_creation.py increase version of google-cloud-pubsub>=2.8.0

If you want to run streaming job decrease version of google-cloud-pubsub==1.7.0

# Trigger to handle finalized files to Google Cloud Storage

Set trigger for Finalizing/Creating to Cloud Storage with BUCKET_ID

Create Cloud Function and put code from cloud_storage/invoke_function.py

Give role for Function to run Cloud Composer Dags.

