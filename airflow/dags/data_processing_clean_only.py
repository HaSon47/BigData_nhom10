from airflow import DAG
# SỬA 1: Import chuẩn cho Airflow 2.x
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import date
import pendulum

# --- CẤU HÌNH ---
SPARK_CONTAINER = "spark-master"

# SỬA 2: Image Bitnami dùng đường dẫn /opt/bitnami/spark/bin/...
SPARK_SUBMIT_CMD = f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit"

# SỬA 3: Image Bitnami có python cài sẵn ở global path
PYTHON_CMD = f"docker exec {SPARK_CONTAINER} python3"

def get_today():
    """Trả về ngày hiện tại theo format ddmmyyyy"""
    today = date.today()
    today_date = today.strftime("%d%m%Y")
    return today_date

default_args = {
    'owner': 'annez',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='data_processing_clean',
    description='Process data using Docker Exec from Airflow',
    start_date=pendulum.yesterday(),
    schedule='0 0 * * *',
    catchup=False
) as dag:

    # TASK 0: Consume data từ Kafka → HDFS
    # Script hdfs_consumer.py đã có timeout 3 phút (180 giây) tích hợp sẵn
    # Nó sẽ tự động dừng sau 3 phút hoặc sau 30 giây không có message mới
    # Chạy trong Airflow container với user root để có access đến packages (kafka-python, hdfs)
    task_consume_kafka_to_hdfs = BashOperator(
        task_id='consume_kafka_to_hdfs',
        bash_command='''
        docker exec -u root airflow bash -c "
        mkdir -p /tmp/hdfs_consumer && \
        cd /home/annez02/BigData_nhom10 && \
        python3 batch/script/hdfs_consumer.py \
          --topic shopee_info \
          --tmp_file /tmp/hdfs_consumer/shopee_local.tmp \
          --dest /user/hadoop/raw \
          --batch_size 1000 \
          --bootstrap_servers kafka_docker-kafka1-1:9095 \
          --timeout 180
        "
        ''',
    )

    # TASK 1: Merge các file JSON từ Kafka consumer thành một file ndjson
    # Script hdfs_consumer tạo nhiều file shopee_data_{timestamp}.json
    # Cần merge thành shopee_raw.ndjson để clean_shopee_data đọc được
    task_merge_raw_files = BashOperator(
        task_id='merge_raw_files',
        bash_command='''
        docker exec namenode bash -c "
        # Download tất cả các file JSON từ HDFS về container
        hdfs dfs -get /user/hadoop/raw/shopee_data_*.json /tmp/ 2>/dev/null || true
        
        # Merge tất cả các file thành một file ndjson (mỗi dòng là một JSON)
        cat /tmp/shopee_data_*.json > /tmp/merged_shopee_raw.ndjson 2>/dev/null || touch /tmp/merged_shopee_raw.ndjson
        
        # Upload file merged lên HDFS
        hdfs dfs -put -f /tmp/merged_shopee_raw.ndjson /user/hadoop/raw/shopee_raw.ndjson
        
        # Cleanup
        rm -f /tmp/shopee_data_*.json /tmp/merged_shopee_raw.ndjson
        "
        '''
    )

    # TASK 2: Tạo thư mục theo ngày và set permission
    task_create_clean_folder = BashOperator(
        task_id='create_today_clean_folder',
        bash_command='docker exec namenode hdfs dfs -mkdir -p /user/hadoop/clean/{{ params.date }}',
        params={'date': get_today()}
    )

    task_grant_clean_access = BashOperator(
        task_id='grant_clean_access',
        bash_command='docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop/clean/{{ params.date }}',
        params={'date': get_today()}
    )

    # TASK 3: Xử lý Shopee
    # Spark Master URL: spark://spark-master:7077 (đã check trong docker-compose-spark.yml)
    # Lưu ý: Script hdfs_consumer tạo file với timestamp, cần merge các file hoặc đọc tất cả
    task_clean_shopee_data = BashOperator(
        task_id="clean_shopee_data",
        bash_command=f"""
        docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 1g \
        /opt/spark-apps/shopee_data.py \
        --origin hdfs://namenode:9000/user/hadoop/raw/shopee_raw.ndjson \
        --destination hdfs://namenode:9000/user/hadoop/clean/{{{{ params.date }}}}/shopee_full_data.csv
        """,
        params={'date': get_today()}
    )

    # TASK 4: Visualize Data
    task_create_visualize_data = BashOperator(
        task_id='create_visualize_data',
        bash_command=f'''
        docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 1g \
        /opt/spark-apps/visualize_data.py \
        --origin hdfs://namenode:9000/user/hadoop/clean/{{{{ params.date }}}}/shopee_full_data.csv \
        --destination hdfs://namenode:9000/user/hadoop/clean/{{{{ params.date }}}}/visualize_data.csv
        ''',
        params={'date': get_today()}
    )

    # TASK 5: Tạo Model Data
    task_create_model_data = BashOperator(
        task_id='create_model_data',
        bash_command=f'''
        docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/model_data.py \
        --shopee hdfs://namenode:9000/user/hadoop/clean/{{{{ params.date }}}}/shopee_full_data.csv \
        --destination hdfs://namenode:9000/user/hadoop/clean/{{{{ params.date }}}}/model_data.csv
        ''',
        params={'date': get_today()}
    )

    # Luồng chạy
    # Consume Kafka (3 phút timeout) → Merge files → Tạo folder → Clean data → Visualize/Model data
    task_consume_kafka_to_hdfs >> task_merge_raw_files
    task_merge_raw_files >> task_create_clean_folder >> task_grant_clean_access
    task_grant_clean_access >> task_clean_shopee_data
    task_clean_shopee_data >> task_create_visualize_data
    task_clean_shopee_data >> task_create_model_data

