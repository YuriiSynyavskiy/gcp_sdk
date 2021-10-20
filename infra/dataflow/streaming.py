import os
import json
import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")

schema = 'id:STRING, dm_gate_id:INT, dm_passcard_id:INT, dm_status_id:INT, dm_direction_id:INT, timestamp:TIMESTAMP, dm_date_id:INT, dm_time_id:STRING'

topic_id = f"projects/{project_id}/topics/{os.environ.get('TOPIC_ID')}"

dataset_id = os.environ.get("DATASET_ID")

passage_table = 'fk_passage'

class ParseToJson(beam.DoFn):

    def process(self, element):
        return [json.loads(element)]

def main(argv=None):

   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.ReadFromPubSub(topic=topic_id).with_output_types(bytes)
      | 'Transform' >> beam.ParDo(ParseToJson())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(f'{project_id}:{dataset_id}.{passage_table}', schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()