import os
import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")

schema = 'gate_id:INT, passcard_id:INT, status_id:INT, direction_id:INT, timestamp:TIMESTAMP'

topic_id = f"projects/{project_id}/topics/{os.environ.get('TOPIC_ID')}"

dataset_id = os.environ.get("DATASET_ID")

passage_table = 'fk_passage'

class Check(beam.DoFn):

    def process(self, element):
        logger.info(element)
        logger.info(dir(element))
        
        return element

def main(argv=None):

   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.ReadFromPubSub(topic=topic_id).with_output_types(bytes)
      | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
      | 'Check' >> beam.ParDo(Check())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(f'{project_id}:{dataset_id}.{passage_table}', schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()