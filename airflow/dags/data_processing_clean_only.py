from airflow import DAG
# SỬA 1: Import chuẩn cho Airflow 2.x
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

# --- CẤU HÌNH ---
SPARK_CONTAINER = "spark-master"

# SỬA 2: Image Bitnami dùng đường dẫn /opt/bitnami/spark/bin/...
SPARK_SUBMIT_CMD = f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit"

# SỬA 3: Image Bitnami có python cài sẵn ở global path
PYTHON_CMD = f"docker exec {SPARK_CONTAINER} python3"

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

    # TASK 1: Xử lý Shopee
    # Spark Master URL: spark://spark-master:7077 (đã check trong docker-compose-spark.yml)
    task_clean_shopee_data = BashOperator(
        task_id="clean_shopee_data",
        bash_command=f"""
        {SPARK_SUBMIT_CMD} \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 1g \
        /opt/spark-apps/shopee_data.py \
        --origin hdfs://namenode:9000/user/hadoop/raw/shopee_raw.ndjson \
        --destination hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv
        """
    )

    # TASK 2: Visualize Data
    task_create_visualize_data = BashOperator(
        task_id='create_visualize_data',
        bash_command=f'''
        {SPARK_SUBMIT_CMD} \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 1g \
        /opt/spark-apps/visualize_data.py \
        --origin hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
        --destination hdfs://namenode:9000/user/hadoop/clean/visualize_data.csv
        '''
    )

    # TASK 3: Tạo Model Data
    task_create_model_data = BashOperator(
        task_id='create_model_data',
        bash_command=f'''
        {SPARK_SUBMIT_CMD} \
        --master spark://spark-master:7077 \
        /opt/spark-apps/model_data.py \
        --shopee hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
        --destination hdfs://namenode:9000/user/hadoop/clean/model_data.csv
        '''
    )

    # Luồng chạy
    task_clean_shopee_data >> task_create_visualize_data
    task_clean_shopee_data >> task_create_model_data