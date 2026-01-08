import ndjson
import random
from confluent_kafka import Producer
import json
import signal
import sys
import time

# Kafka configuration
kafka_bootstrap_servers = "localhost:9094"
# kafka_topics = ["shopee_info", "lazada_info"]  # List of Kafka topics
kafka_topics = ["shopee_info"]  # List of Kafka topics

# Path to the NDJSON file
PATH_FILE_NDJSON = '/mnt/disk1/hachi/BigData_nhom10/data/combined.ndjson'
TIME_INTERVAL = 60
# Initialize Kafka producer
producer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers
}
producer = Producer(producer_conf)

# Function to handle delivery reports from Kafka


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to read data and send a random record to Kafka


def send_random_record():
    with open(PATH_FILE_NDJSON, 'r') as file:
        data = ndjson.load(file)
    while True:
        random_record = random.choice(data)
        record_dict = dict(random_record)
        for topic in kafka_topics:
            # breakpoint()
            producer.produce(topic, key=None, value=json.dumps(
                record_dict), callback=delivery_report)
        producer.flush()
        print("Random record sent to Kafka topics.")
        # Wait for 2 seconds before sending the next record
        time.sleep(TIME_INTERVAL)

# Signal handler for graceful shutdown


def send_each_record():
    with open(PATH_FILE_NDJSON, 'r') as file:
        data = ndjson.load(file)
    for record in data:
        record_dict = dict(record)
        for topic in kafka_topics:
            producer.produce(topic, key=None, value=json.dumps(
                record_dict), callback=delivery_report)
        producer.flush()
        print("Record sent to Kafka topics.")


def signal_handler(sig, frame):
    print('You pressed Ctrl+C! Exiting gracefully...')
    producer.flush()  # Ensure all messages are sent before exiting
    sys.exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Main function to send data
if __name__ == "__main__":
    send_random_record()
    # send_each_record()
