To create trigger for incoming files use command below:

gcloud functions deploy trigger_dag_function --runtime=python39 --trigger-resource=<BUCKET_ID> --trigger-event=google.storage.object.finalize --memory=128 --timeout=20 --max-instances=100 --set-env-vars WEBSERVER_URL=<AIRFLOW_WEBSERVER_URL>

To delete trigger use command below:

gcloud function delete trigger_dag_function
