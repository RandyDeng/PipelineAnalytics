import generator
import time
from google.cloud import pubsub_v1
from pipelineanalytics import PROJECT, TOPIC_NAME


SLEEP_TIME = 1


def publish_message(project=PROJECT, topic_name=TOPIC_NAME):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    data = generator.generate_post()
    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)

    print('Published message: {}'.format(data))


def simulate_datastream():
    while True:
        publish_message(PROJECT, TOPIC_NAME)
        time.sleep(SLEEP_TIME)


if __name__ == '__main__':
   simulate_datastream()