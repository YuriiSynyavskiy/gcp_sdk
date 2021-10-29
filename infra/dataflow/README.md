This module will receive messages from PubSub topic and push into BigQuery table - fk_passage.

First of all be sure that you have created Cloud Storage bucket with id such in .env (you can do this with running script at cloud_storage folder)

Batch and Stream processing from PubSub to Bigquery with Python.

https://towardsdatascience.com/lets-build-a-streaming-data-pipeline-e873d671fc57

Running of job:

"""
export PROJECT_ID=

export BUCKET_ID=

python streaming.py \
--runner DataFlow \
--project $PROJECT_ID \
--temp_location gs://$BUCKET_ID/tmp \
--staging_location gs://$BUCKET_ID/staging
--region europe-west6
--job_name streaming_pub_sub_to_bigquery
--streaming
"""

python main.py