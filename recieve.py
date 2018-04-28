import argparse
import apache_beam as beam
import itertools
import os
import csv
#import random
#import string
#import time
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

####################################################################################
# This funtion will generate an output file where each line is designed to simulate
#    an online post, with the data items in each line comma-delineated
def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record['mac']), (record['status']), (record['datetime'])

def run(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read the pubsub topic into a PCollection.
    lines = ( p | beam.io.ReadStringsFromPubSub(subscription="projects/pipelineanalytics-202518/subscriptions/dataflow")
               # | beam.Map(parse_pubsub)
               # | beam.Map(lambda (mac_bq, status_bq, datetime_bq): {'mac': mac_bq, 'status': status_bq, 'datetime': datetime_bq})
               # | beam.io.WriteToBigQuery(
               #    known_args.output_table,
               #     schema=' mac:STRING, status:INTEGER, datetime:TIMESTAMP',
               #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )
#    print(lines)

if __name__ == '__main__':
  run()
