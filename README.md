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
   - Mặc định Kibana được map tới cổng `5061` → truy cập: `http://localhost:5061`
   - Nếu cần mở port trên firewall (Ubuntu):
     ```bash
     sudo ufw allow 5061/tcp
     ```

---

## Kiểm tra & gỡ rối ⚠️
- Kiểm tra logs của các container:
  - `docker compose logs -f`
- Đảm bảo Kafka và Elasticsearch đã chạy trước khi khởi động producer hoặc Spark job.
- Nếu gặp lỗi kết nối Elasticsearch, kiểm tra username/password và trạng thái cluster (bằng `curl` ở trên).

---

## Batch Layer 📦
- **Đang phát triển** — sẽ cập nhật hướng dẫn khi hoàn thiện.

---

## Dừng dịch vụ & dọn dẹp 🧹
- Dừng và xóa volumes (nếu cần):
  ```bash
  cd docker/elk_single-node_docker
  docker compose down -v
  ```


---

Nếu bạn muốn, tôi có thể bổ sung các lệnh cụ thể cho từng docker-compose file hoặc thêm ví dụ cấu hình Spark/Makefile. 🔧