from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import date
import pendulum


default_args = {
    'owner': 'jazzdung',
    'retries':5,
    'retry_delay': timedelta(minutes=20)
}

def get_this_week():
    today = date.today()
    today_date = today.strftime("%m%Y")
    week = str(int(today.strftime("%d"))//7 +1)
    return today_date+ "_" +week


with DAG(
    dag_id='data_crawling',
    description='Crawl data',
    start_date=pendulum.yesterday(),
    schedule_interval='0 0 * * 1',
    catchup=True
) as dag:

    task_create_raw_folder = BashOperator(
        task_id='create_today_raw_folder',
        bash_command='HADOOP_USER_NAME=hadoop /home/jazzdung/hadoop/bin/hdfs dfs -mkdir /user/hadoop/raw/{{ params.week }}',
        params = {'week' : get_this_week()}
    )

    task_grant_access = BashOperator(
        task_id='grant_access',
        bash_command='HADOOP_USER_NAME=hadoop /home/jazzdung/hadoop/bin/hdfs dfs -chown -R jazzdung /user/hadoop/raw/{{ params.week }}',
        params = {'week' : get_this_week()}
    )

    task_crawl_shopee_url = BashOperator(
        task_id='crawl_shopee_url',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site shopee --type url --num_page 1'
    )

    task_crawl_lazada_url = BashOperator(
        task_id='crawl_lazada_url',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site lazada --type url --num_page 1'
    )

    task_crawl_shopee_data_1 = BashOperator(
        task_id='crawl_shopee_data_1',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site shopee --type info --consumer_id 1'
    )

    task_crawl_shopee_data_2 = BashOperator(
        task_id='crawl_shopee_data_2',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site shopee --type info --consumer_id 2'
    )

    task_crawl_lazada_data_1 = BashOperator(
        task_id='crawl_lazada_data_1',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site lazada --type info --consumer_id 1'
    )

    task_crawl_lazada_data_2 = BashOperator(
        task_id='crawl_lazada_data_2',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/main.py --site lazada --type info --consumer_id 2'
    )

    task_lazada_kafka = BashOperator(
        task_id='lazada_kafka',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/batch/script/hdfs_consumer.py --topic lazada_info --tmp_file /home/jazzdung/tmp_lazada --dest hdfs://viet:9000/user/hadoop/raw/{{ params.week }}/lazada_raw.ndjson',
        params = {'week' : get_this_week()}
    )

    task_shopee_kafka = BashOperator(
        task_id='shopee_kafka',
        bash_command='python3 /mnt/disk1/hachi/BigData_nhom10/batch/script/hdfs_consumer.py --topic shopee_info --tmp_file /home/jazzdung/tmp_shopee --dest hdfs://viet:9000/user/hadoop/raw/{{ params.week }}/shopee_raw.ndjson',
        params = {'week' : get_this_week()}
    )

    task_create_raw_folder >> task_grant_access >> [task_crawl_shopee_url , task_crawl_shopee_data_1, task_crawl_shopee_data_2, task_crawl_lazada_url, task_crawl_lazada_data_1, task_crawl_lazada_data_2, task_lazada_kafka, task_shopee_kafka]