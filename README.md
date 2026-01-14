# BigData_nhom10

## Tổng quan ✅
Repo này chứa các thành phần cho **Stream Layer** (đã sẵn sàng) và **Batch Layer** (đang phát triển). Hướng dẫn dưới đây giúp bạn dựng môi trường, giả lập dữ liệu streaming tới Kafka, xử lý bằng Spark và đẩy kết quả tới Elasticsearch để Kibana visualize.

---

## Yêu cầu trước 🔧
- Docker & Docker Compose
- Python 3.8+
- (Tùy chọn) Java nếu Spark cần chạy cục bộ
- Cài Python packages: `pip install -r requirements.txt`

---

## Stream Layer — Hướng dẫn chạy (Ngắn gọn) 🔁
1. Cài đặt các thư viện Python cần thiết:
   - `python -m pip install -r requirements.txt`

2. Chạy Docker Compose để dựng các thành phần (Kafka, Elasticsearch, Kibana,...):
   - Di chuyển vào thư mục chứa docker-compose (ví dụ `docker/elk_1node_docker`) và chạy:
     ```bash
     cd docker/elk_1node_docker
     docker compose up -d
     ```
   - Kiểm tra Elasticsearch:
     ```bash
     curl -u elastic:123123 http://localhost:9200/_cluster/health?pretty
     ```
- Cần chạy 2 file yml trong:
    - elk_1node_docker: dựng elasticsearch, kibana
    - kafka_docker: dựng kafka

3. Giả lập dữ liệu real-time tới Kafka:
   - Chạy producer stream:
     ```bash
     python stream/producer_stream.py
     ```
   - Producer sẽ gửi dữ liệu giả lập vào topic đã cấu hình.

4. Chạy pipeline Spark để xử lý dữ liệu và gửi kết quả tới Elasticsearch:
   - Sử dụng `Makefile` trong thư mục `stream`:
     ```bash
     cd stream
     make
     ```
   - Lệnh `make` sẽ thực thi job Spark (theo cấu hình trong repo) và đẩy kết quả tới Elasticsearch.

5. Mở Kibana để visualize dữ liệu streaming:
   - Mặc định Kibana được map tới cổng `5061` → truy cập: `http://localhost:5601`
   - Nếu cần mở port trên firewall (Ubuntu):
     ```bash
     sudo ufw allow 5061/tcp
     ```

---

## Batch Layer 📦

### Tổng quan kiến trúc

Batch Layer xử lý dữ liệu theo các giai đoạn:

```
Kafka Topics → HDFS Consumer → Raw Data (HDFS) 
    ↓
Data Cleaning (Spark) → Clean Data (HDFS)
    ↓
Model Data Prep (Spark) → Model Data (HDFS)
    ↓
Visualize Data (Spark) → Visualize Data (HDFS)
    ↓
Model Training (Spark) → Trained Models
```

**Các thành phần chính:**
- **HDFS Consumer** (`batch/script/hdfs_consumer.py`): Đọc từ Kafka → ghi vào HDFS raw
- **Data Cleaning** (`batch/script/shopee_data.py`, `lazada_data.py`): Clean và transform raw data
- **Model Data Prep** (`batch/script/model_data.py`): Merge và chuẩn bị data cho training
- **Visualize Data** (`batch/script/visualize_data.py`): Group và deduplicate cho visualization (chỉ ghi HDFS, không còn Elasticsearch)
- **Model Training** (`batch/model/model.py`): Train ML models (Linear Regression, Random Forest, GBT)

**Cấu trúc lưu trữ dữ liệu:**
- **Raw data**: `/user/hadoop/raw/` (không phân theo ngày)
- **Clean data** (Airflow DAG): `/user/hadoop/clean/{ddmmyyyy}/` (theo ngày)
- **Clean data** (Manual): `/user/hadoop/clean/` (trực tiếp)

---

### Prerequisites

1. **Docker & Docker Compose** đã cài đặt
2. **Python 3.8+** với các packages:
   ```bash
   pip install -r requirements.txt
   ```
3. **Kafka đã chạy** (từ Stream Layer)
4. **Network Docker** `bigdata_net` đã được tạo:
   ```bash
   docker network create bigdata_net
   ```

---

### Bước 1: Khởi động các services

#### 1.1. Khởi động Hadoop (HDFS)

```bash
cd docker/hadoop_docker
docker compose -f docker-compose.yml up -d
```

**Kiểm tra HDFS:**
```bash
# Kiểm tra namenode
docker exec namenode hdfs dfs -ls /

# Hoặc truy cập Web UI: http://localhost:9870
```

#### 1.2. Khởi động Spark

```bash
cd docker/hadoop_docker
docker compose -f docker-compose-spark.yml up -d
```

**Kiểm tra Spark:**
```bash
# Kiểm tra Spark Master
docker exec spark-master /opt/spark/bin/spark-submit --version

# Hoặc truy cập Web UI: http://localhost:8070
```

#### 1.3. Khởi động Airflow (tùy chọn - để orchestrate)

```bash
cd airflow
docker compose -f docker-compose-airflow.yml up -d
```

**Kiểm tra Airflow:**
- Web UI: http://localhost:8082
- Username: `admin`
- Password: `admin`

---

### Bước 2: Tạo cấu trúc thư mục HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/raw
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/clean
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/model
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/vis
docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop
```

**Lưu ý về cấu trúc thư mục theo ngày:**
- Khi chạy qua **Airflow DAG**, dữ liệu sẽ được lưu tự động vào thư mục theo ngày: `/user/hadoop/clean/{ddmmyyyy}/`
- Format ngày: `ddmmyyyy` (ví dụ: `14012026` cho ngày 14/01/2026)
- DAG sẽ tự động tạo thư mục và set permission trước khi chạy tasks
- Khi chạy **manual**, bạn có thể chọn lưu trực tiếp vào `/user/hadoop/clean/` hoặc tạo thư mục ngày thủ công

---

### Bước 3: Chạy Batch Pipeline

Có 2 cách chạy: **Manual** (từng bước) hoặc **Airflow DAG** (tự động).

#### Cách 1: Chạy Manual (từng bước)

**3.1. Consume data từ Kafka → HDFS**

```bash
python3 batch/script/hdfs_consumer.py \
  --topic shopee_info \
  --tmp_file /tmp/shopee_local.tmp \
  --dest /user/hadoop/raw/shopee_raw.ndjson \
  --batch_size 1000
```

**Lưu ý:** Script sẽ tự động tìm Hadoop container và ghi batch vào các file timestamped.

**3.2. Clean Shopee data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  /opt/spark-apps/shopee_data.py \
  --origin hdfs://namenode:9000/user/hadoop/raw/shopee_raw.ndjson \
  --destination hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv
```

**3.3. Tạo Model Data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/model_data.py \
  --shopee hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
  --destination hdfs://namenode:9000/user/hadoop/clean/model_data.csv
```

**3.4. Tạo Visualize Data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  /opt/spark-apps/visualize_data.py \
  --origin hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
  --destination hdfs://namenode:9000/user/hadoop/clean/visualize_data.csv
```

**3.5. Train Model**

```bash
python3 batch/model/model.py \
  --mode train \
  --model_name lr \
  --save_dir /tmp/model_checkpoint/ \
  --train_csv_path hdfs://namenode:9000/user/hadoop/clean/model_data.csv \
  --cross_validation 3 \
  --hyperparam_tuning True
```

#### Cách 2: Chạy với Airflow DAG (tự động)

**3.1. Copy scripts vào Spark container (nếu chưa mount)**

Scripts đã được mount vào `/opt/spark-apps` trong `docker-compose-spark.yml`.

**3.2. Trigger DAG**

- Truy cập Airflow UI: http://localhost:8082
- Tìm DAG `data_processing_clean` hoặc `data_processing`
- Click "Play" để trigger

**Hoặc dùng CLI:**

```bash
# List DAGs
airflow dags list

# Trigger DAG
airflow dags trigger data_processing_clean

# Xem logs
airflow tasks logs data_processing_clean clean_shopee_data 2026-01-14
```

**Cấu trúc dữ liệu khi chạy qua Airflow DAG:**

DAG `data_processing_clean` sẽ tự động:
1. Tạo thư mục theo ngày: `/user/hadoop/clean/{ddmmyyyy}/` (ví dụ: `/user/hadoop/clean/14012026/`)
2. Set permission 777 cho thư mục
3. Lưu các file output vào thư mục ngày:
   - `shopee_full_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/shopee_full_data.csv`
   - `visualize_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/visualize_data.csv`
   - `model_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/model_data.csv`

**Lợi ích:**
- Tổ chức dữ liệu theo ngày, dễ quản lý và truy vết
- Tránh ghi đè dữ liệu giữa các ngày
- Dễ dàng xóa dữ liệu cũ theo ngày nếu cần

---

### Bước 4: Kiểm tra kết quả

**Xem files trên HDFS:**

```bash
# List raw data
docker exec namenode hdfs dfs -ls /user/hadoop/raw/

# List clean data (nếu chạy manual - không có thư mục ngày)
docker exec namenode hdfs dfs -ls /user/hadoop/clean/

# List clean data theo ngày (nếu chạy qua Airflow DAG)
# Thay {ddmmyyyy} bằng ngày thực tế, ví dụ: 14012026
docker exec namenode hdfs dfs -ls /user/hadoop/clean/14012026/

# Xem tất cả các thư mục ngày
docker exec namenode hdfs dfs -ls /user/hadoop/clean/

# Download file để kiểm tra (manual)
docker exec namenode hdfs dfs -get /user/hadoop/clean/shopee_full_data.csv /tmp/

# Download file từ thư mục ngày (Airflow DAG)
docker exec namenode hdfs dfs -get /user/hadoop/clean/14012026/shopee_full_data.csv /tmp/
```

**Hoặc dùng Web UI:**
- HDFS: http://localhost:9870 → Browse the file system
- Spark: http://localhost:8070 → Xem jobs và applications

---

### Troubleshooting

#### Lỗi: `ModuleNotFoundError: No module named 'pyspark'`
- **Nguyên nhân:** Chạy PySpark script bằng `python3` thay vì `spark-submit`
- **Giải pháp:** Luôn dùng `spark-submit` cho các script PySpark

#### Lỗi: `ConnectTimeoutException: Call From ... to namenode:9000 failed`
- **Nguyên nhân:** Hostname `namenode` không được resolve
- **Giải pháp:** Thêm vào `/etc/hosts`:
  ```bash
  echo "127.0.0.1 namenode" | sudo tee -a /etc/hosts
  ```

#### Lỗi: `JAVA_HOME is not set`
- **Nguyên nhân:** Java chưa được cài hoặc JAVA_HOME chưa set
- **Giải pháp:**
  ```bash
  sudo apt install -y openjdk-11-jdk
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  export PATH=$JAVA_HOME/bin:$PATH
  ```

#### Lỗi: `Nothing has been added to this summarizer` (khi train model)
- **Nguyên nhân:** Dataset quá nhỏ cho cross-validation
- **Giải pháp:** Script tự động điều chỉnh số folds hoặc skip CV nếu data quá nhỏ

#### Lỗi: Permission denied trên HDFS
- **Giải pháp:**
  ```bash
  docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop
  ```

#### Xem logs của containers
```bash
# Xem logs Hadoop
docker compose -f docker-compose.yml logs -f

# Xem logs Spark
docker compose -f docker-compose-spark.yml logs -f

# Xem logs Airflow
cd airflow
docker compose -f docker-compose-airflow.yml logs -f
```

---

### Links hữu ích

- **Airflow UI:** http://localhost:8082/
- **HDFS NameNode UI:** http://localhost:9870/
- **Spark Master UI:** http://localhost:8070/
- **Hadoop ResourceManager:** http://localhost:8088/ (nếu có)

---

### Dừng services

```bash
# Dừng Airflow
cd airflow
docker compose -f docker-compose-airflow.yml down

# Dừng Spark
cd docker/hadoop_docker
docker compose -f docker-compose-spark.yml down

# Dừng Hadoop
docker compose -f docker-compose.yml down

# Xóa volumes (cẩn thận - sẽ mất data)
docker compose -f docker-compose.yml down -v
```
---

## Dừng dịch vụ & dọn dẹp 🧹
- Dừng và xóa volumes (nếu cần):
  ```bash
  cd docker/elk_single-node_docker
  docker compose down -v
  ```


---

