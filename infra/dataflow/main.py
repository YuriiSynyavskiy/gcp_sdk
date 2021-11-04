import os
from google.cloud import logging, storage
import json
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

schema = 'id:STRING, dm_gate_key:INT, dm_passcard_key: INT, dm_status_id:INT, dm_direction_id:INT, timestamp:TIMESTAMP'

passage_table = 'fk_passage'

bucket_id = 'edu-passage-bucket'
p_options = {
    'project':'data-n-analytics-edu',
    'region':'us-west4',
    'staging_location': f'gs://{bucket_id}/staging',
    'temp_location': f'gs://{bucket_id}/tmp',
    'runner': 'DataflowRunner',
    'setup_file': './setup.py',
    'save_main_session': True,
    'streaming': True
}


topic_name = 'passage-events'
topic_id = f"projects/{p_options['project']}/topics/{topic_name}"

dataset_id = "landing_passage_dataset"


class ParseToJson(beam.DoFn):
    def start_bundle(self):
       LOG_NAME = f"pub-sub-{topic_name}"
       logging_client = logging.Client()
       self.logger = logging_client.logger(LOG_NAME)
       client=storage.Client()  
       self.bucket=client.get_bucket(bucket_id);

    def process(self, element):
        record = json.loads(element)
        record_correctness = True
        try:
            if record.get('id') and record.get('dm_gate_key') and record.get('dm_passcard_key'):
                record_correctness = True
            else:
                record_correctness = False
        except Exception:
            record_correctness = False
        if record_correctness:
            self.logger.log_struct({
                'message': f"{datetime.now(tz=None)} Record {record.get('id')} will be uploaded to BigQuery",
                'id': record.get('id')
            }, severity="INFO")
            return [record]
        else:
            self.logger.log_struct({
                'message': f"{datetime.now(tz=None)} Error with record {record.get('id')}",
                'record': record
            }, severity="ERROR")
            blob=self.bucket.blob(f'error_records/{record["id"]}_record.txt')
            blob.upload_from_string(json.dumps(record))
        return

pipeline_options = PipelineOptions(flags=[], **p_options)
def run():
    with beam.Pipeline(options=pipeline_options) as p:
        (p
            | 'ReadData' >> beam.io.ReadFromPubSub(topic=topic_id).with_output_types(bytes)
            | 'Transform' >> beam.ParDo(ParseToJson())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(f'{p_options["project"]}:{dataset_id}.{passage_table}', schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
if __name__ == '__main__':
    run()
