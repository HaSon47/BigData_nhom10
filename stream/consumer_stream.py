from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import signal
import sys

# Kafka configuration
kafka_bootstrap_servers = "localhost:9094"
kafka_group_id = "1"  # Replace with your Kafka consumer group ID

# Initialize Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': kafka_group_id,
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(consumer_conf)

# Signal handler for graceful shutdown


def signal_handler(sig, frame):
    print('You pressed Ctrl+C! Exiting gracefully...')
    consumer.close()
    sys.exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Function to consume messages


def consume_messages(kafka_topics):
    print("Subscribed to topics: ", kafka_topics)
    consumer.subscribe(kafka_topics)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Message successfully received
                record = json.loads(msg.value().decode('utf-8'))
                print(f"Received message from topic {msg.topic()}: {record}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the consumer gracefully
        consumer.close()


# Main function to start consuming messages
if __name__ == "__main__":
    print("Starting Kafka consumer...")
    kafka_topics = ["shopee_info", "lazada_info"]  # List of Kafka topics
    consume_messages(kafka_topics)
