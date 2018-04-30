import analysis
import time
import queue
from google.cloud import pubsub_v1


PROJECT = 'pipelineanalytics-202518'
TOPIC_NAME = 'twitter-ingest'
SUBSCRIPTION = 'dataflow'
ANALYSIS_INTERVAL = 30


message_q = queue.Queue()


def receive_message(message):



if __name__ == '__main__':
    # Begin callback loop to receive messages

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background
    while True:
        # Analyze the data in the queue every ANALYSIS_INTERVAL

