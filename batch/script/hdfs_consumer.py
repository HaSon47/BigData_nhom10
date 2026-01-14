import logging
import subprocess
import argparse
import time
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9094']
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Kafka consumer with HDFS sink')
parser.add_argument('--topic', type=str, help='Kafka topic')
parser.add_argument('--tmp_file', type=str, help='Local temp file path')
parser.add_argument('--dest', type=str, help='Hdfs destination path')
parser.add_argument('--batch_size', type=int, default=1000, help='Number of messages to batch before writing')

args = parser.parse_args()

# Buffer để batch messages
message_buffer = []

def check_hdfs_command():
    """Check if hdfs command is available"""
    try:
        subprocess.run(['hdfs', 'dfs', '-ls', '/'], 
                      capture_output=True, check=True, timeout=5)
        return 'local'
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None

def find_hadoop_container():
    """Tự động tìm Hadoop namenode container"""
    logging.info("Searching for Hadoop namenode container...")
    
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'],
            capture_output=True, text=True, check=True
        )
        containers = result.stdout.strip().split('\n')
        
        for container in containers:
            if 'namenode' in container.lower():
                logging.info(f"Found namenode container: {container}")
                return container
    except Exception as e:
        logging.warning(f"Error searching containers: {e}")
    
    common_names = ['namenode', 'hadoop_docker-namenode-1', 'hadoop-namenode']
    for name in common_names:
        try:
            result = subprocess.run(
                ['docker', 'exec', name, 'hdfs', 'dfs', '-ls', '/'],
                capture_output=True, text=True, check=True, timeout=5
            )
            logging.info(f"Successfully found container: {name}")
            return name
        except:
            continue
    
    logging.error("Could not find any namenode container")
    return None

def check_docker_hdfs(container_name):
    """Check if HDFS is running in Docker"""
    try:
        result = subprocess.run(
            ['docker', 'exec', container_name, 'hdfs', 'dfs', '-ls', '/'],
            capture_output=True, text=True, check=True, timeout=5
        )
        return True
    except:
        return False

def write_batch_to_hdfs(messages, container_name, max_retries=3):
    """Ghi batch messages vào HDFS bằng cách TẠO FILE MỚI (Put)"""
    
    # Tạo tên file tạm ở local
    batch_file = f"{args.tmp_file}_batch"
    with open(batch_file, 'w', encoding='utf-8') as f:
        for msg in messages:
            f.write(msg + '\n')
    
    # TẠO TÊN FILE ĐÍCH TRÊN HDFS (Dựa theo thời gian để không trùng)
    timestamp = int(time.time())
    # Lưu ý: args.dest lúc này phải là đường dẫn THƯ MỤC
    hdfs_dest_path = f"{args.dest.rstrip('/')}/shopee_data_{timestamp}.json"

    logging.info(f"Uploading batch to new file: {hdfs_dest_path}")

    # Copy file vào container (bước trung gian)
    tmp_container_path = f"/tmp/batch_{timestamp}.tmp"
    subprocess.run(['docker', 'cp', batch_file, f'{container_name}:{tmp_container_path}'], check=False)

    # Dùng lệnh PUT thay vì appendToFile
    for attempt in range(max_retries):
        try:
            result = subprocess.run([
                'docker', 'exec', container_name,
                'hdfs', 'dfs', '-put', tmp_container_path, hdfs_dest_path
            ], capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                logging.info(f"Success: Wrote {len(messages)} msgs to {hdfs_dest_path}")
                
                # Dọn dẹp file tạm trong container cho sạch
                subprocess.run(['docker', 'exec', container_name, 'rm', tmp_container_path], check=False)
                
                # Dọn dẹp file tạm ở local
                try:
                    import os
                    if os.path.exists(batch_file):
                        os.remove(batch_file)
                        logging.info(f"Cleaned up local temp file: {batch_file}")
                except Exception as e:
                    logging.warning(f"Failed to remove local temp file: {e}")
                
                return True
            else:
                logging.warning(f"Attempt {attempt+1} failed: {result.stderr}")
                time.sleep(2)
        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(2)

    return False

def flush_buffer(container_name):
    """Ghi tất cả messages trong buffer vào HDFS"""
    if not message_buffer:
        return
    
    if write_batch_to_hdfs(message_buffer, container_name):
        message_buffer.clear()
    else:
        logging.error(f"Failed to write batch, keeping {len(message_buffer)} messages in buffer")

def write_to_hdfs(msg, container_name=None):
    """Thêm message vào buffer hoặc ghi ngay nếu buffer đầy"""
    global message_buffer
    
    # Thêm vào buffer
    message_buffer.append(msg)
    
    # Nếu buffer đầy hoặc chưa có container, tìm container
    if container_name is None:
        container_name = find_hadoop_container()
        if not container_name or not check_docker_hdfs(container_name):
            logging.warning("Cannot find HDFS container. Writing to local file only.")
            write_to_local(msg)
            return
    
    # Nếu buffer đầy, flush ngay
    if len(message_buffer) >= args.batch_size:
        flush_buffer(container_name)
    
    return container_name

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
    
    container_name = None
    terminate_count = 0
    last_flush_time = time.time()
    flush_interval = 30  # Flush buffer mỗi 30 giây nếu chưa đầy
    
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
                logging.info(f"partition: {record.partition}, offset: {record.offset}")
                container_name = write_to_hdfs(record.value, container_name)
            
            # Flush buffer nếu đã quá thời gian
            if time.time() - last_flush_time > flush_interval:
                if message_buffer:
                    logging.info(f"Flushing buffer after {flush_interval}s interval")
                    flush_buffer(container_name)
                    last_flush_time = time.time()
        else:
            logging.info("No message found")
            terminate_count += 1
            
            # Flush buffer trước khi terminate
            if terminate_count >= 900:
                if message_buffer:
                    logging.info("Flushing remaining messages before exit")
                    flush_buffer(container_name)
                break
            
            # Flush buffer định kỳ ngay cả khi không có message mới
            if time.time() - last_flush_time > flush_interval:
                if message_buffer:
                    logging.info(f"Flushing buffer after {flush_interval}s interval")
                    flush_buffer(container_name)
                    last_flush_time = time.time()
    
    consumer.close()
    logging.info("Consumer closed")