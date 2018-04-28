import generator
import time
from google.cloud import pubsub_v1


PROJECT = 'pipelineanalytics-202518'
TOPIC_NAME = 'twitter-ingest'


def publish_message(project, topic_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    data = generator.generate_post()
    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)

    print('Published messages.')


def simulate_datastream():
    while True:
        publish_message(PROJECT, TOPIC_NAME)
        time.sleep(1)
