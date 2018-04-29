import analysis
import time
import queue
from google.cloud import pubsub_v1


PROJECT = 'pipelineanalytics-202518'
TOPIC_NAME = 'twitter-ingest'
SUBSCRIPTION = 'dataflow'
ANALYSIS_INTERVAL = 10


message_q = queue.Queue()


def receive_message(message):
    # print('Received message: {}'.format(message))
    message.ack()
    message_q.put(message)


if __name__ == '__main__':
    # Begin callback loop to receive messages
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT, SUBSCRIPTION)
    subscriber.subscribe(subscription_path, callback=receive_message)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background
    while True:
        # Analyze the data in the queue every ANALYSIS_INTERVAL
        time.sleep(ANALYSIS_INTERVAL)
        size = message_q.qsize()
        messages = [message_q.get().data for i in range(size)]
        analysis.batch(messages)
