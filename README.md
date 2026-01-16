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
Kafka Topics (shopee_info)
    ↓
HDFS Consumer (hdfs_consumer.py)
    → Tạo nhiều file timestamped: shopee_data_{timestamp}.json
    → Lưu vào /user/hadoop/raw/
    → Timeout: 180 giây (3 phút) hoặc dừng sau 30s không có message
    ↓
Merge Raw Files (trong namenode container)
    → Merge tất cả shopee_data_*.json → shopee_raw.ndjson
    ↓
Create Clean Folder (theo ngày: {ddmmyyyy})
    → Tạo /user/hadoop/clean/{ddmmyyyy}/ và set permission 777
    ↓
Data Cleaning (Spark - shopee_data.py)
    → Đọc shopee_raw.ndjson
    → Clean và transform
    → Ghi shopee_full_data.csv vào /user/hadoop/clean/{ddmmyyyy}/
    ↓
    ├─→ Visualize Data (Spark - visualize_data.py)
    │   → Group và deduplicate
    │   → Ghi visualize_data.csv vào /user/hadoop/clean/{ddmmyyyy}/
    │
    └─→ Model Data Prep (Spark - model_data.py)
        → Merge và chuẩn bị data cho training
        → Ghi model_data.csv vào /user/hadoop/clean/{ddmmyyyy}/
```

**Các thành phần chính:**
- **HDFS Consumer** (`batch/script/hdfs_consumer.py`): 
  - Consume messages từ Kafka topic `shopee_info`
  - Tạo nhiều file timestamped: `shopee_data_{timestamp}.json` trong `/user/hadoop/raw/`
  - Tự động dừng sau 180 giây hoặc sau 30 giây không có message mới
  - Chạy trong Airflow container với user root (cần `kafka-python` và `hdfs` packages)
- **Merge Raw Files**: Merge tất cả file JSON timestamped thành `shopee_raw.ndjson` (chạy trong namenode container)
- **Data Cleaning** (`batch/script/shopee_data.py`): Clean và transform raw data từ ndjson → CSV
- **Model Data Prep** (`batch/script/model_data.py`): Merge và chuẩn bị data cho training (chỉ xử lý Shopee, Lazada optional)
- **Visualize Data** (`batch/script/visualize_data.py`): Group và deduplicate cho visualization (chỉ ghi HDFS, không còn Elasticsearch)
- **Model Training** (`batch/model/model.py`): Train ML models (Linear Regression, Random Forest, GBT) - chạy manual

**Cấu trúc lưu trữ dữ liệu:**
- **Raw data**: `/user/hadoop/raw/` 
  - File timestamped từ consumer: `shopee_data_{timestamp}.json`
  - File merged: `shopee_raw.ndjson`
- **Clean data** (Airflow DAG): `/user/hadoop/clean/{ddmmyyyy}/` (theo ngày)
  - Format ngày: `ddmmyyyy` (ví dụ: `16012026` cho ngày 16/01/2026)
  - Files: `shopee_full_data.csv`, `visualize_data.csv`, `model_data.csv`
- **Clean data** (Manual): `/user/hadoop/clean/` (trực tiếp, không phân theo ngày)

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
  --dest /user/hadoop/raw \
  --batch_size 1000 \
  --bootstrap_servers localhost:9094 \
  --timeout 180 \
  --hdfs_host namenode \
  --hdfs_port 9870
```

**Lưu ý:** 
- Script sẽ tạo nhiều file timestamped: `shopee_data_{timestamp}.json` trong `/user/hadoop/raw/`
- Script tự động dừng sau 180 giây hoặc sau 30 giây không có message mới
- Cần có `kafka-python` và `hdfs` packages: `pip install kafka-python hdfs`

**3.2. Merge các file JSON thành một file ndjson**

```bash
docker exec namenode bash -c "
  hdfs dfs -get /user/hadoop/raw/shopee_data_*.json /tmp/ 2>/dev/null || true
  cat /tmp/shopee_data_*.json > /tmp/merged_shopee_raw.ndjson 2>/dev/null || touch /tmp/merged_shopee_raw.ndjson
  hdfs dfs -put -f /tmp/merged_shopee_raw.ndjson /user/hadoop/raw/shopee_raw.ndjson
  rm -f /tmp/shopee_data_*.json /tmp/merged_shopee_raw.ndjson
"
```

**3.3. Clean Shopee data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  /opt/spark-apps/shopee_data.py \
  --origin hdfs://namenode:9000/user/hadoop/raw/shopee_raw.ndjson \
  --destination hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv
```

**3.4. Tạo Model Data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/model_data.py \
  --shopee hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
  --destination hdfs://namenode:9000/user/hadoop/clean/model_data.csv
```

**Lưu ý:** Script `model_data.py` chỉ xử lý Shopee data. Nếu muốn merge với Lazada, thêm `--lazada` argument.

**3.5. Tạo Visualize Data**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  /opt/spark-apps/visualize_data.py \
  --origin hdfs://namenode:9000/user/hadoop/clean/shopee_full_data.csv \
  --destination hdfs://namenode:9000/user/hadoop/clean/visualize_data.csv
```

**3.6. Train Model (chạy manual, không có trong DAG)**

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

**3.1. Đảm bảo Spark containers đang chạy**

Trước khi trigger DAG, kiểm tra Spark containers:

```bash
# Kiểm tra trạng thái
docker ps | grep spark

# Nếu containers đã dừng, start lại
docker start spark-master spark-worker-1

# Đợi vài giây để Spark khởi động hoàn toàn
sleep 5
```

**3.2. Copy scripts vào Spark container (nếu chưa mount)**

Scripts đã được mount vào `/opt/spark-apps` trong `docker-compose-spark.yml`.

**3.3. Trigger DAG**

- Truy cập Airflow UI: http://localhost:8082
- Username: `admin`, Password: `admin`
- Tìm DAG `data_processing_clean`
- Click "Play" để trigger

**Hoặc dùng CLI:**

```bash
# List DAGs
docker exec airflow airflow dags list

# Trigger DAG
docker exec airflow airflow dags trigger data_processing_clean

# Xem logs
docker exec airflow airflow tasks logs data_processing_clean clean_shopee_data 2026-01-16(year-month-day)
```

**Cấu trúc DAG `data_processing_clean`:**

DAG sẽ chạy các tasks theo thứ tự:

1. **`consume_kafka_to_hdfs`**: 
   - Consume từ Kafka topic `shopee_info` (bootstrap: `kafka_docker-kafka1-1:9095`)
   - Tạo các file `shopee_data_{timestamp}.json` trong `/user/hadoop/raw/`
   - Timeout: 180 giây (3 phút)
   - Chạy trong Airflow container với user root

2. **`merge_raw_files`**: 
   - Merge tất cả `shopee_data_*.json` → `shopee_raw.ndjson`
   - Chạy trong namenode container

3. **`create_today_clean_folder`**: 
   - Tạo thư mục `/user/hadoop/clean/{ddmmyyyy}/` (format: `ddmmyyyy`)

4. **`grant_clean_access`**: 
   - Set permission 777 cho thư mục ngày

5. **`clean_shopee_data`**: 
   - Clean và transform data từ `shopee_raw.ndjson`
   - Ghi `shopee_full_data.csv` vào `/user/hadoop/clean/{ddmmyyyy}/`

6. **`create_visualize_data`** và **`create_model_data`** (chạy song song):
   - Visualize: Group và deduplicate → `visualize_data.csv`
   - Model: Merge và prepare → `model_data.csv`
   - Cả hai đều lưu vào `/user/hadoop/clean/{ddmmyyyy}/`

**Cấu trúc dữ liệu khi chạy qua Airflow DAG:**

DAG `data_processing_clean` sẽ tự động:
1. Tạo thư mục theo ngày: `/user/hadoop/clean/{ddmmyyyy}/` (ví dụ: `/user/hadoop/clean/16012026/`)
2. Set permission 777 cho thư mục
3. Lưu các file output vào thư mục ngày:
   - `shopee_full_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/shopee_full_data.csv`
   - `visualize_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/visualize_data.csv`
   - `model_data.csv` → `/user/hadoop/clean/{ddmmyyyy}/model_data.csv`

**Lợi ích:**
- Tổ chức dữ liệu theo ngày, dễ quản lý và truy vết
- Tránh ghi đè dữ liệu giữa các ngày
- Dễ dàng xóa dữ liệu cũ theo ngày nếu cần
- Tự động hóa toàn bộ pipeline từ Kafka → HDFS → Clean → Model/Visualize

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

#### Lỗi: `container is not running` (khi chạy Spark jobs)
- **Nguyên nhân:** Spark containers (`spark-master`, `spark-worker-1`) đã dừng
- **Giải pháp:**
  ```bash
  # Kiểm tra trạng thái
  docker ps -a | grep spark
  
  # Start lại containers
  docker start spark-master spark-worker-1
  
  # Đợi vài giây để Spark khởi động
  sleep 5
  
  # Kiểm tra lại
  docker ps | grep spark
  ```
- **Phòng tránh:** Có thể thêm `restart: always` vào docker-compose-spark.yml

#### Lỗi: `ModuleNotFoundError: No module named 'kafka'` (khi chạy hdfs_consumer)
- **Nguyên nhân:** Thiếu packages `kafka-python` hoặc `hdfs` trong Airflow container
- **Giải pháp:** Packages đã được cài trong `docker-compose-airflow.yml`. Nếu vẫn lỗi:
  ```bash
  docker exec -u root airflow pip install kafka-python hdfs
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

