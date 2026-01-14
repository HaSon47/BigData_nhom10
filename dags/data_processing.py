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

def get_today():
    today = date.today()
    today_date = today.strftime("%d%m%Y")
    return today_date

with DAG(
    dag_id='data_processing',
    description='Process data',
    start_date=pendulum.yesterday(),
    schedule_interval='0 0 * * *',
    catchup=True
) as dag:

    task_create_clean_folder = BashOperator(
        task_id='create_today_clean_folder',
        bash_command='docker exec namenode hdfs dfs -mkdir -p /user/hadoop/clean/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_create_model_folder = BashOperator(
        task_id='create_today_model_folder',
        bash_command='docker exec namenode hdfs dfs -mkdir -p /user/hadoop/model/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_create_vis_folder = BashOperator(
        task_id='create_today_vis_folder',
        bash_command='docker exec namenode hdfs dfs -mkdir -p /user/hadoop/vis/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_grant_clean_access = BashOperator(
        task_id='grant_clean_access',
        bash_command='docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop/clean/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_grant_model_access = BashOperator(
        task_id='grant_model_access',
        bash_command='docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop/model/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_grant_vis_access = BashOperator(
        task_id='grant_vis_access',
        bash_command='docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop/vis/{{ params.date }}',
        params = {'date' : get_today()}
    )

    task_clean_shopee_data = BashOperator(
        task_id='clean_shopee_data',
        bash_command='cd /home/annez02/BigData_nhom10 && python3 batch/script/shopee_data.py --origin hdfs://namenode:9000/user/hadoop/raw/{{ params.week }}/shopee_raw.ndjson --destination hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/shopee_full_data.csv',
        params = {'week' : get_this_week(), 'date' : get_today()}
    )

    task_clean_lazada_data = BashOperator(
        task_id='clean_lazada_data',
        bash_command='cd /home/annez02/BigData_nhom10 && python3 batch/script/lazada_data.py --origin hdfs://namenode:9000/user/hadoop/raw/{{ params.week }}/lazada_raw.ndjson --destination hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/lazada_full_data.csv',
        params = {'week' : get_this_week(), 'date' : get_today()}
    )

    task_create_visualize_data = BashOperator(
        task_id='create_visualize_data',
        bash_command='cd /home/annez02/BigData_nhom10 && python3 batch/script/visualize_data.py --origin hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/shopee_full_data.csv --destination hdfs://namenode:9000/user/hadoop/vis/{{ params.date }}/visualize_data.csv',
        params = {'date' : get_today()}
    )

    task_create_model_data = BashOperator(
        task_id='create_model_data',
        bash_command='cd /home/annez02/BigData_nhom10 && python3 batch/script/model_data.py --shopee hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/shopee_full_data.csv --lazada hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/lazada_full_data.csv --destination hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/model_data.csv',
        params = {'date' : get_today()}
    )

    task_train_model = BashOperator(
        task_id='train_model',
        bash_command='cd /home/annez02/BigData_nhom10 && python3 batch/model/model.py --mode train --model_name lr --save_dir /tmp/model_checkpoint/{{ params.date }}/ --train_csv_path hdfs://namenode:9000/user/hadoop/clean/{{ params.date }}/model_data.csv --cross_validation 3 --hyperparam_tuning True',
        params = {'date' : get_today()}
    )

    task_create_clean_folder >> task_grant_clean_access
    task_create_model_folder >> task_grant_model_access
    task_create_vis_folder >> task_grant_vis_access
    task_grant_clean_access >> task_clean_shopee_data
    task_grant_clean_access >> task_clean_lazada_data
    [task_clean_shopee_data, task_grant_vis_access] >> task_create_visualize_data
    [task_clean_shopee_data, task_clean_lazada_data] >> task_create_model_data

    [task_grant_model_access, task_create_model_data] >> task_train_model