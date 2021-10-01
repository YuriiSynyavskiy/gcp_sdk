# gcp_sdk

To run queries:

Go to IAM&Admin -> Service accounts, choose existing service or generate new with your roles.

Click Actions -> Manage keys -> Add key -> Put google-credentials.json to ~/.

In terminal define path to your file:

export export GOOGLE_APPLICATION_CREDENTIALS="/home/<your-user>/.google-cloud-push-events-keys.json"

You need generate file .env where must be structure:

PROJECT_ID=<project-id>

TOPIC_ID=<topic-name>

SCHEMA_ID=<schema-name>

DATASET_ID=<dataset-name>

Generate topic in Pub Sub with script - pub_sub_topic_creation.py.

Generate schema of BigQuery with script - bigquery_schema.py