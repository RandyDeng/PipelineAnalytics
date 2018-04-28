import argparse
import apache_beam as beam


def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record['mac']), (record['status']), (record['datetime'])


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        print("THIS IS SPARTA")
        # Read the pubsub topic into a PCollection.
    lines = (
        p
        | beam.io.ReadStringsFromPubSub(subscription="projects/pipelineanalytics-202518/subscriptions/dataflow")
    )

    # | beam.Map(parse_pubsub)
    # | beam.Map(lambda (mac_bq, status_bq, datetime_bq): {'mac': mac_bq, 'status': status_bq, 'datetime': datetime_bq})
    # | beam.io.WriteToBigQuery(
    #    known_args.output_table,
    #     schema=' mac:STRING, status:INTEGER, datetime:TIMESTAMP',
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    run()
