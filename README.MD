# Real-Time Credit Card Fraud Detection Lakehouse Pipeline

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1.0-00ADEE?style=for-the-badge&logo=delta&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72C48?style=for-the-badge&logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-3.0.2-20A7C9?style=for-the-badge&logo=apache&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---

## 📌 معرفی پروژه (Overview)

سامانه‌ی **کشف تقلب برخط کارت‌های اعتباری مبتنی بر معماری Lakehouse**، یک زیرساخت پردازش داده‌های عظیم (Big Data) در مقیاس صنعتی است که با هدف حذف تاخیر زمانی (Data Staleness) و تضمین شفافیت داده‌ها بر پایه‌ی ذخیره‌سازی **Delta Lake** و موتور پردازش استریمینگ **Apache Spark** طراحی و پیاده‌سازی شده است.

این پروژه به عنوان پروژه پایانی درس **پردازش داده‌های عظیم (دوره کارشناسی ارشد)** پیاده‌سازی شده و قادر است هزاران تراکنش در ثانیه (TPS) را بلادرنگ دریافت، پیش‌پردازش، ارزیابی هوشمند (مدل یادگیری ماشین) و ذخیره کند و نتایج را بدون لایه‌های پیچیده‌ی ETL در اختیار ابزارهای تحلیلی نظیر **Apache Superset** قرار دهد.

---

## 🏗️ معماری سیستم و جریان داده (Architecture & Pipeline)

معماری پایپ‌لاین بر پایه‌ی الگوی **Stream-to-Lakehouse** پیاده‌سازی شده و تمام اجزای آن به صورت **Containerized** درون محیط Docker اجرا می‌شوند:

```text
[ CreditCard CSV Dataset ]
           │
           ▼
[ Phase 0: 00_eda_and_preprocessing.py ] ───────► (RobustScaler Scaling)
           │
           ▼
[ Phase 2: 02_kafka_producer.py ] ──────────────► (10 & 40 TPS + Event Timestamp Injection)
           │
           ▼
[ Apache Kafka KRaft Cluster ] ─────────────────► (Topic: creditcard-transactions)
           │
           ▼
[ Phase 3: 03_spark_structured_streaming.py ] ──► (10s Watermark + 1m Sliding Window + Checkpointing)
           │                                      │
           ├───► [ Phase 4: Random Forest ML ] ───┼─► (Cost-Sensitive Class Weight = 290.0)
           │                                      │
           ▼                                      ▼
[ Delta Lake Storage on MinIO S3A ] ─────────────► (ACID Transactions + Parquet Files)
           │
           ├─────────────────────────────────────────┐
           ▼                                         ▼
[ Phase 5: 05_evaluation_metrics.py ]      [ Phase 6: export_delta_to_postgres.py ]
 (Sub-second Latency & ML Benchmarks)                │
                                                     ▼
                                           [ PostgreSQL Database ] (Table: transactions)
                                                     │
                                                     ▼
                                           [ Apache Superset Dashboard ] (Port 8088)
```

---

## 🛠️ تکنولوژی‌ها و ابزارهای استفاده‌شده (Tech Stack)

| لایه کاربردی | ابزار / تکنولوژی | توضیحات |
| :--- | :--- | :--- |
| **Ingestion & Messaging** | **Apache Kafka (KRaft Mode)** | پیام‌رسان توزیع‌شده برای دریافت و صف‌بندی استریم تراکنش‌ها |
| **Stream Processing** | **Apache Spark Structured Streaming** | موتور پردازش بلادرنگ با قابلیت Watermarking و Sliding Windows |
| **Lakehouse Storage** | **Delta Lake + MinIO (S3A)** | لایه ذخیره‌سازی شیءمحور با پشتیبانی از تراکنش‌های ACID و Time Travel |
| **Machine Learning** | **Spark MLlib & MLflow** | مدل Random Forest با راهکار Cost-Sensitive Learning جهت رفع Class Imbalance |
| **Data Relational Storage** | **PostgreSQL 15** | دیتابیس رابطه‌ای انبار داده جهت همگام‌سازی زنده و پاسخگویی به Superset |
| **Data Visualization** | **Apache Superset** | داشبورد تحلیلی و کوئری‌گیری برخط Direct Access |
| **Containerization** | **Docker & Docker Compose** | مدیریت ارکستراسیون یکپارچه‌ی تمام کانتینرها |

---

## 📁 ساختار پروژه (Project Structure)

```text
.
├── docker-compose.yml              # کانفیگ کانتینرهای Spark, Kafka, MinIO, PostgreSQL, Superset
├── superset_config.py              # تنظیمات اختصاصی Apache Superset
├── requirements.txt                # نیازمندی‌های پایتون (PySpark, Delta, Kafka-Python, psycopg2)
├── docs/                           # مستندات و گزارش رسمی پروژه
│   ├── 00_theoretical_foundation.md
│   └── FINAL_REPORT.md
└── scripts/                        # اسکریپت‌های ۶ گانه اجرای پایپ‌لاین
    ├── 00_eda_and_preprocessing.py # فاز ۰: پیش‌پردازش و آماده‌سازی داده‌ها
    ├── 01_lakehouse_minio_delta.py  # فاز ۱: ایجاد S3 Bucket و ساختار Delta Lake
    ├── 02_kafka_producer.py        # فاز ۲: ارسال استریم تراکنش‌ها به کافکا
    ├── 03_spark_structured_streaming.py # فاز ۳: پردازش پنجره‌ای استریمینگ و ذخیره در دلتا
    ├── 04_ml_train_random_forest.py # فاز ۴: آموزش مدل Random Forest و ثبت آزمایش‌ها
    ├── 05_evaluation_metrics.py    # فاز ۵: سنجش تاخیر پردازش و معیارهای ارزیابی ML
    ├── 06_lakehouse_visualization.py# فاز ۶: کوئری‌گیری مستقیم Spark SQL روی Delta Lake
    └── export_delta_to_postgres.py # فاز ۶ مكمل: همگام‌سازی داده‌های Delta به PostgreSQL
```

---

## ⚡ راهنمای راه‌اندازی و اجرای گام‌به‌گام (Execution Guide)

### ۱. پیش‌نیازها (Prerequisites)
* سیستم‌عامل Linux / macOS / Windows Subsystem for Linux (WSL2)
* Docker Engine (نسخه ۲۰ به بالا) و Docker Compose
* حداقل ۸ گیگابایت حافظه RAM آزاد

### ۲. راه‌اندازی کانتینرهای زیرساخت (Docker Compose)
دستور زیر تمام کانتینرهای Spark Master/Worker, Kafka, MinIO, PostgreSQL و Superset را اجرا می‌کند:

```bash
docker compose up -d
```

برای بررسی وضعیت سلامت کانتینرها:
```bash
docker compose ps
```

---

### ۳. اجرای اسکریپت‌های پایپ‌لاین (Phase 0 to Phase 6)

#### 🔹 فاز ۰: پیش‌پردازش داده‌ها (EDA & Scaling)
ایجاد ویژگی‌های نرمال‌شده با `RobustScaler` و آماده‌سازی مجموعه داده:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/00_eda_and_preprocessing.py
```

#### 🔹 فاز ۱: راه‌اندازی سطل MinIO و جدول Delta Lake
ایجاد باکت `lakehouse-fraud` در MinIO با قابلیت ACID:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/01_lakehouse_minio_delta.py
```

#### 🔹 فاز ۲: تزریق داده‌ها با Kafka Producer
ارسال استریم تراکنش‌ها به کافکا با نرخ ۱۰ TPS (یا ۴0 TPS برای حالت پیک):
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/02_kafka_producer.py --tps 10
```

#### 🔹 فاز ۳: پردازش استریمینگ و ذخیره در Delta Lake
اجرای Spark Structured Streaming با واترمارک ۱۰ ثانیه‌ای:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/03_spark_structured_streaming.py
```

#### 🔹 فاز ۴: آموزش مدل یادگیری ماشین (Random Forest)
آموزش مدل تشخیص تقلب با اعمال `classWeight = 290.0`:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/04_ml_train_random_forest.py
```

#### 🔹 فاز ۵: ارزیابی کارایی و تاخیر (Latency & ML Evaluation)
سنجش تاخیر زیر ثانیه‌ای و استخراج ماتریس درهم‌ریختگی:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/05_evaluation_metrics.py
```

#### 🔹 فاز ۶: همگام‌سازی Delta Lake به PostgreSQL جهت اتصال به Apache Superset
انتقال داده‌های پردازش‌شده به جدول `transactions` درون PostgreSQL:
```bash
docker exec -it spark_master_lakehouse python3 /app/scripts/export_delta_to_postgres.py
```

---

## 📊 نتایج ارزیابی و مقایسه با مقاله Baseline

### ۱. معیارهای کیفی یادگیری ماشین (ML Metrics Comparison)

| معیار ارزیابی (Metric) | مقاله پایه (Baseline Paper) | پروژه ما (Our Lakehouse) | میزان بهبود |
| :--- | :---: | :---: | :---: |
| **Precision (دقت)** | **0.34** | **0.81** | **+138% بهبود چشمگیر** |
| **Recall (فراخوانی)** | **0.88** | **0.86** | حفظ نرخ بالا |
| **F1-Score** | **0.49** | **0.83** | **+69% بهبود** |
| **ROC-AUC** | **0.91** | **0.96** | **+5% بهبود** |

> **علت جهش Precision:** استفاده از استراتژی **Cost-Sensitive Learning (وزن‌دهی ۲۹۰)** به جای روش‌های مرسوم Oversampling مصنوعی که باعث کاهش شدید اعلام‌های اشتباه (False Positives) گردید.

### ۲. تاخیر پردازش برخط (End-to-End Latency)

| نرخ ورود داده (Throughput) | تاخیر مقاله پایه | تاخیر سیستم ما | بهبود کارایی |
| :--- | :---: | :---: | :---: |
| **10 TPS (حالت نرمال)** | 0.80 ثانیه | **0.42 ثانیه** | **۴۷٪ سریع‌تر** |
| **40 TPS (حالت پیک)** | 0.90 ثانیه | **0.58 ثانیه** | **۳۵٪ سریع‌تر** |

---

## 📈 مشاهده داشبورد در Apache Superset

1. مرورگر خود را باز کرده و به آدرس زیر بروید:
   `http://localhost:8088`
2. با اطلاعات ورود اولیه وارد شوید (`admin` / `admin`).
3. یک Database Connection جدید از نوع **PostgreSQL** اضافه کنید:
   * **SQLAlchemy URI:** `postgresql://admin:adminpassword@postgres:5432/fraud_lakehouse`
4. کوئری زیر را در SQL Lab جهت سنجش نرخ تقلب در پنجره‌های زمانی اجرا کنید:
```sql
SELECT 
    window_start,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) as fraud_count,
    AVG("Amount") as avg_amount
FROM transactions
GROUP BY window_start
ORDER BY window_start DESC
LIMIT 10;
```

---

## 🔧 چالش‌های حل‌شده در طول توسعه (Troubleshooting)

1. **برطرف کردن خطای Py4J (`ModuleNotFoundError: No module named 'py4j'`):**
   * **راهکار:** تزریق برنامه‌نویسی‌شده مسیرهای کانتینری PySpark و فایل‌های ZIP Py4J موجود در `/opt/bitnami/spark/python` به `sys.path`.
2. **رفع محدودیت اتصالات دیتابیس در Apache Superset:**
   * **راهکار:** افزودن سرویس کانتینری PostgreSQL 15 به Docker Compose و انتقال JDBC داده‌های Delta Lake به PostgreSQL جهت دور زدن محدودیت امنیتی SQLite.
3. **جلوگیری از خطای OOM در پردازش استریمینگ:**
   * **راهکار:** تعبیه Watermark ۱۰ ثانیه‌ای (`withWatermark("event_time", "10 seconds")`) جهت پاکسازی خودکار State Store اسپارک.

---

## 📜 لایسنس و اطلاعات پژوهشگر

* **پژوهشگر / توسعه‌دهنده:** حسین مستاجران
* **مقطع:** کارشناسی ارشد مهندسی کامپیوتر
* **درس:** پردازش داده‌های عظیم (Big Data)
* **لایسنس:** MIT License
