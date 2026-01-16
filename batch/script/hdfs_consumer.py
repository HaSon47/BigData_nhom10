#!/usr/bin/env python3
"""
Kafka Consumer script để consume messages từ Kafka và ghi vào HDFS.
Script này sẽ tự động dừng sau 3 phút (180 giây) để đảm bảo DAG có thể tiếp tục.
"""

import argparse
import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from hdfs import InsecureClient

# Default timeout: 3 phút (180 giây)
DEFAULT_TIMEOUT = 180

def upload_to_hdfs(hdfs_client, local_file, hdfs_path):
    """Upload file từ local lên HDFS"""
    try:
        hdfs_client.upload(hdfs_path, local_file, overwrite=False)
        print(f"Uploaded {local_file} to {hdfs_path}")
        return True
    except Exception as e:
        print(f"Error uploading to HDFS: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Consume Kafka messages and write to HDFS')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic name')
    parser.add_argument('--tmp_file', type=str, required=True, help='Local temporary file path')
    parser.add_argument('--dest', type=str, required=True, help='HDFS destination directory')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size before uploading to HDFS')
    parser.add_argument('--bootstrap_servers', type=str, default='localhost:9094', help='Kafka bootstrap servers')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT, help=f'Timeout in seconds (default: {DEFAULT_TIMEOUT})')
    parser.add_argument('--hdfs_host', type=str, default='namenode', help='HDFS namenode host')
    parser.add_argument('--hdfs_port', type=int, default=9870, help='HDFS namenode port')
    
    args = parser.parse_args()
    
    # Tạo thư mục cho tmp_file nếu chưa có
    os.makedirs(os.path.dirname(args.tmp_file), exist_ok=True)
    
    # Kết nối HDFS
    hdfs_url = f'http://{args.hdfs_host}:{args.hdfs_port}'
    hdfs_client = InsecureClient(hdfs_url, user='hadoop')
    
    # Tạo Kafka consumer
    print(f"Connecting to Kafka at {args.bootstrap_servers}")
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers.split(','),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000  # 1 giây timeout cho mỗi poll
    )
    
    print(f"Consumer started. Timeout: {args.timeout} seconds")
    
    batch = []
    batch_count = 0
    start_time = time.time()
    last_message_time = start_time
    terminate_count = 0
    
    try:
        while True:
            # Kiểm tra timeout tuyệt đối (3 phút)
            elapsed_time = time.time() - start_time
            if elapsed_time >= args.timeout:
                print(f"Timeout reached ({args.timeout} seconds). Stopping consumer.")
                break
            
            # Poll messages từ Kafka
            message_pack = consumer.poll(timeout_ms=1000)
            
            if message_pack:
                # Có messages
                terminate_count = 0
                last_message_time = time.time()
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        batch.append(message.value)
                        
                        # Khi đủ batch_size, upload lên HDFS
                        if len(batch) >= args.batch_size:
                            timestamp = int(time.time())
                            batch_file = f"{args.tmp_file}.{timestamp}.json"
                            
                            # Ghi batch vào file local
                            with open(batch_file, 'w') as f:
                                for item in batch:
                                    f.write(json.dumps(item) + '\n')
                            
                            # Upload lên HDFS với tên file có timestamp
                            hdfs_filename = f"shopee_data_{timestamp}.json"
                            hdfs_path = f"{args.dest}/{hdfs_filename}"
                            
                            if upload_to_hdfs(hdfs_client, batch_file, hdfs_path):
                                # Xóa file local sau khi upload thành công
                                try:
                                    os.remove(batch_file)
                                except:
                                    pass
                            
                            batch_count += 1
                            batch = []
                            print(f"Uploaded batch {batch_count} to HDFS")
            else:
                # Không có message mới
                terminate_count += 1
                
                # Nếu không có message trong 30 giây, dừng lại
                if time.time() - last_message_time > 30:
                    print("No messages for 30 seconds. Stopping consumer.")
                    break
        
        # Upload batch còn lại nếu có
        if batch:
            timestamp = int(time.time())
            batch_file = f"{args.tmp_file}.{timestamp}.json"
            
            with open(batch_file, 'w') as f:
                for item in batch:
                    f.write(json.dumps(item) + '\n')
            
            hdfs_filename = f"shopee_data_{timestamp}.json"
            hdfs_path = f"{args.dest}/{hdfs_filename}"
            
            if upload_to_hdfs(hdfs_client, batch_file, hdfs_path):
                try:
                    os.remove(batch_file)
                except:
                    pass
            print(f"Uploaded final batch to HDFS")
    
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        consumer.close()
        print(f"Consumer closed. Total batches uploaded: {batch_count}")

if __name__ == '__main__':
    main()


