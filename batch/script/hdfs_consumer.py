import logging
import subprocess
import argparse
from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = ['viet:9092', 'jazzdung:9092', 'dungbruh:9092']
# BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093']

logging.basicConfig(level=logging.INFO)


parser = argparse.ArgumentParser(description='Kafka consumer with HDFS sink')

parser.add_argument('--topic', type=str,
                help='Kafka topic')
parser.add_argument('--tmp_file', type=str,
                help='Local temp file path')
parser.add_argument('--dest', type=str,
                help='Hdfs destination path')

args = parser.parse_args()


def write_to_hdfs(msg):
    write_to_local(msg)
    subprocess.run(['hdfs', 'dfs', '-appendToFile', args.tmp_file, args.dest])


def write_to_local(msg):
    with open(args.tmp_file, "w", encoding='utf-8') as f:
        f.write(msg + '\n')

if __name__ == '__main__':
    consumer = KafkaConsumer(
                        bootstrap_servers=BOOTSTRAP_SERVERS,
                        value_deserializer=lambda v: v.decode('utf-8'),
                        group_id=args.topic
                    )
    logging.info("Consumer created")

    consumer.subscribe([args.topic])
    logging.info(f"Subscribed to topic: {args.topic}")
    terminate_count = 0
    while True:
        logging.info("Polling for messages")
        d = consumer.poll(timeout_ms=1000, max_records=1)
        logging.info("Polling completed")
        if d:
            terminate_count = 0
            logging.info("Message found")
            records = list(d.values())[0]
            logging.info("Number of messages: {}".format(len(records)))

            for record in records:
                logging.info(f"partition: {record.partition}, offset: {record.offset}, value: {record.value}")
                write_to_hdfs(record.value)
        else:
            logging.info("No message found")
            terminate_count += 1
            if terminate_count == 900:
                break
