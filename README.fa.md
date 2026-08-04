# 🏦 پایپ‌لاین تشخیص تقلب برخط کارت‌های اعتباری با معماری Lakehouse

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1.0-00ADEE?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72C48?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Superset](https://img.shields.io/badge/Apache%20Superset-3.0.2-20A7C9?style=for-the-badge&logo=apache&logoColor=white)](https://superset.apache.org/)
[![MLflow](https://img.shields.io/badge/MLflow-2.10.2-red?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)](LICENSE)

---

[📖 **نسخه‌ی فارسی**](./README.fa.md) | [📖 **English Version**](./README.md)

---

## 📌 معرفی پروژه

این پروژه یک **سیستم تشخیص تقلب برخط (Real-Time) در مقیاس صنعتی** است که بر پایه‌ی **معماری Lakehouse** طراحی و پیاده‌سازی شده است. این سیستم با یکپارچه‌سازی **پردازش جریان داده (Streaming)**، **یادگیری ماشین** و **تحلیل تعاملی (Interactive Analytics)** بر روی یک پلتفرم باز و یکپارچه، امکان شناسایی تراکنش‌های مشکوک را در کسری از ثانیه فراهم می‌کند.

> 🎯 **دستاورد کلیدی**: کاهش **~۴۷ درصدی تأخیر (Latency)** نسبت به معماری‌های سنتی دو-لایه (Two-Tier)، همراه با دستیابی به **دقت (Precision) ۸۱٪** و **حساسیت (Recall) ۸۶٪** بر روی دیتاست بسیار نامتوازن (نسبت کلاهبرداری: ۰٫۱۷۲٪).

این پروژه به‌عنوان **پروژه‌ی پایانی درس پردازش داده‌های عظیم (کارشناسی ارشد)** در دانشگاه اصفهان پیاده‌سازی شده است و قادر است هزاران تراکنش در ثانیه را به‌صورت بلادرنگ دریافت، پیش‌پردازش، تحلیل هوشمند (با مدل یادگیری ماشین) و ذخیره‌سازی کند و نتایج را بدون نیاز به پایپ‌لاین‌های پیچیده‌ی ETL در اختیار ابزارهای تحلیلی مانند Apache Superset قرار دهد.

---

## 🏗️ معماری سیستم و جریان داده

معماری پایپ‌لاین بر اساس الگوی **Stream-to-Lakehouse** طراحی شده و تمامی اجزای آن به‌صورت **کانتینری (Containerized)** در محیط Docker اجرا می‌شوند.

```text
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         REAL-TIME FRAUD DETECTION LAKEHOUSE PIPELINE                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│                                ┌──────────────────┐                                 │
│                                │  Credit Card CSV │                                 │
│                                │     Dataset      │                                 │
│                                └────────┬─────────┘                                 │
│                                         │                                           │
│                                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 0: EDA & Preprocessing (00_eda_and_preprocessing.py)                   │   │
│  │ • RobustScaler on 'Amount' & 'Time'  • Stratified 70/15/15 split             │   │
│  └──────────────────────────────────────┬───────────────────────────────────────────│
│                                         │                                           │
│                                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 2: Kafka Producer (02_kafka_producer.py)                               │   │
│  │ • Configurable 10/40 TPS  • Wall-clock timestamp injection (ISO-8601 UTC)    │   │
│  └──────────────────────────────────────┬───────────────────────────────────────────│
│                                         │                                           │
│                                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │           Apache Kafka Cluster (KRaft Mode – No ZooKeeper)                   │   │
│  │                              Topic: creditcard-transactions                  │   │
│  └──────────────────────────────────────┬───────────────────────────────────────────│
│                                         │                                           │
│                                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 3: Spark Structured Streaming (03_spark_structured_streaming.py)       │   │
│  │ • Watermark: 10s  • Sliding Window: 1m duration, 30s slide                   │   │
│  │ • Feature Engineering: tx_count, total_amount, avg_amount per window         │   │
│  │ • Checkpointing for Exactly-Once semantics                                   │   │
│  └────────┬─────────────────────────────────────────────────────────────────────┘   │
│           │                                                                         │
│           ├─────────────────────────────┬────────────────────────────────┐          │
│           ▼                             ▼                                ▼          │
│  ┌───────────────────────┐  ┌─────────────────────────────────┐  ┌────────────────┐ │
│  │  Delta Lake on MinIO  │  │ Phase 4: ML Training & Inference│  │  Phase 5:      │ │
│  │  (S3A)                │  │ (04_ml_train_random_forest.py)  │  │  Evaluation    │ │
│  │  • ACID Transactions  │  │ • Random Forest (100 trees)     │  │  (Latency &    │ │
│  │  • Time Travel        │  │ • Cost-Sensitive Learning       │  │   ML Metrics)  │ │
│  │  • Schema Evolution   │  │   (Class Weight = 290.0)        │  │                │ │
│  │  • Z-ORDER Indexing   │  │ • MLflow Model Registry         │  └─────────────── ┘ │
│  └───────────┬───────────┘  │ • Real-time streaming inference │                     │
│              │              │   with fraud alerts to Kafka    │                     │
│              │              └─────────────────────────────────┘                     │
│              │                                                                      │
│              │                                                                      │
│              │                                                                      │
│              ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────┐       │
│  │ Phase 6: Direct Access & Visualization                                   │       │
│  │ • export_delta_to_postgres.py → PostgreSQL 15 (fraud_lakehouse DB)       │       │
│  │ • Apache Superset Dashboard (Port 8088) – 4 interactive widgets          │       │
│  │ • Direct Spark SQL queries on Delta Lake (Zero Staleness)                │       │
│  └──────────────────────────────────────────────────────────────────────────┘       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘   
```

---

## 🛠️ تکنولوژی‌ها و ابزارهای استفاده‌شده

| لایه کاربردی | ابزار / تکنولوژی | توضیحات |
| :--- | :--- | :--- |
| **ورود و پیام‌رسانی** | **Apache Kafka (حالت KRaft)** | پیام‌رسان توزیع‌شده برای دریافت و صف‌بندی جریان تراکنش‌ها |
| **پردازش جریان** | **Apache Spark Structured Streaming** | موتور پردازش بلادرنگ با قابلیت Watermarking و پنجره‌های لغزان |
| **ذخیره‌سازی Lakehouse** | **Delta Lake + MinIO (S3A)** | لایه ذخیره‌سازی شیءمحور با پشتیبانی از تراکنش‌های ACID و Time Travel |
| **یادگیری ماشین** | **Spark MLlib و MLflow** | مدل Random Forest با راهکار Cost-Sensitive Learning برای رفع عدم‌توازن کلاس‌ها |
| **ذخیره‌سازی رابطه‌ای** | **PostgreSQL 15** | دیتابیس رابطه‌ای برای همگام‌سازی داده‌ها و پاسخگویی به Superset |
| **مصورسازی داده** | **Apache Superset** | داشبورد تحلیلی و کوئری‌گیری برخط با دسترسی مستقیم (Direct Access) |
| **مدیریت کانتینرها** | **Docker & Docker Compose** | ارکستراسیون یکپارچه‌ی تمام کانتینرها |

---

## 📁 ساختار پروژه

```
Real-Time-Fraud-Detection-Lakehouse/
├── scripts/
│   ├── 00_eda_and_preprocessing.py          # فاز ۰: تحلیل و پیش‌پردازش
│   ├── 01_lakehouse_minio_delta.py          # فاز ۱: راه‌اندازی Delta Lake روی MinIO
│   ├── 02_kafka_producer.py                 # فاز ۲: تولید جریان داده در کافکا
│   ├── 03_spark_structured_streaming.py     # فاز ۳: پردازش استریمینگ و پنجره‌بندی
│   ├── 04_ml_train_random_forest.py         # فاز ۴: آموزش مدل و ثبت در MLflow
│   ├── 05_evaluation_metrics.py             # فاز ۵: ارزیابی تأخیر و معیارهای ML
│   ├── 06_lakehouse_visualization.py        # فاز ۶: دسترسی مستقیم و کوئری‌گیری
│   └── export_delta_to_postgres.py          # همگام‌سازی Delta Lake به PostgreSQL
├── sql/
│   ├── widget-01.sql                        # کوئری‌های ویجت‌های Superset
│   ├── widget-02.sql
│   ├── widget-03.sql
│   └── widget-04.sql
├── data_output/                             # خروجی‌های تولیدشده (CSV, JSON)
├── docker-compose.yml                       # ارکستراسیون کامل کانتینرها
├── requirements.txt                         # وابستگی‌های پایتون
├── superset_config.py                       # تنظیمات اختصاصی Superset
├── .gitignore
└── README.md
```

---

## 📊 نتایج ارزیابی و مقایسه با مقاله Baseline

### ۱. معیارهای کیفی مدل یادگیری ماشین

| معیار ارزیابی | مقاله پایه (Baseline) | پروژه ما (Lakehouse) | میزان بهبود |
| :--- | :---: | :---: | :---: |
| **دقت (Precision)** | **0.34** | **0.81** | **+۱۳۸٪** |
| **حساسیت (Recall)** | **0.88** | **0.86** | حفظ نرخ بالا |
| **نمره‌ی F1** | **0.49** | **0.83** | **+۶۹٪** |
| **ROC-AUC** | **0.94** | **0.96** | **+۲٪** |

> **دلیل جهش دقت (Precision):** استفاده از استراتژی **یادگیری هزینه‌محور (Cost-Sensitive Learning)** با وزن ۲۹۰٫۰ برای کلاس کلاهبرداری (به‌جای روش‌های مصنوعی Oversampling) باعث کاهش چشمگیر هشدارهای اشتباه (False Positives) شده است.

### ۲. تأخیر پردازش سرتاسر (End-to-End Latency)

| نرخ ورود داده | تأخیر مقاله پایه | تأخیر سیستم ما | بهبود کارایی |
| :--- | :---: | :---: | :---: |
| **۱۰ TPS (حالت عادی)** | ۰٫۸۰ ثانیه | **۰٫۴۲ ثانیه** | **۴۷٫۵٪ سریع‌تر** |
| **۴۰ TPS (حالت پیک)** | ۰٫۹۰ ثانیه | **۰٫۵۸ ثانیه** | **۳۵٫۶٪ سریع‌تر** |

> **دلیل کاهش تأخیر:** حذف گلوگاه‌های ETL، استفاده از فرمت ستونی Parquet در Delta Lake، و اجرای کانتینری Spark با جداسازی بهینه‌ی منابع.

---

## 🚀 راهنمای اجرای گام‌به‌گام

### پیش‌نیازها
- Docker Desktop (یا Docker Engine + Compose)
- Python 3.12+
- Git

### ۱. دریافت و نصب

```bash
git clone https://github.com/HosseinMst81/Real-Time-Fraud-Detection-Lakehouse.git
cd Real-Time-Fraud-Detection-Lakehouse

# ایجاد محیط مجازی
python -m venv venv
source venv/Scripts/activate      # ویندوز: venv\Scripts\activate

# نصب وابستگی‌ها (در صورت نیاز از میرور داخلی استفاده کنید)
pip install -r requirements.txt -i https://pypi.iranrepo.ir/simple
```

### ۲. راه‌اندازی سرویس‌ها

```bash
docker compose up -d
```

بررسی وضعیت کانتینرها:
```bash
docker compose ps
```

**آدرس‌های دسترسی به سرویس‌ها:**

| سرویس | آدرس | نام کاربری / رمز |
|-------|------|------------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadminpassword |
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark Worker UI** | http://localhost:8082 | - |
| **Kafka Broker** | localhost:9092 | - |
| **Schema Registry** | http://localhost:8081 | - |
| **MLflow UI** | http://localhost:5000 | - |
| **Apache Superset** | http://localhost:8088 | admin / admin |
| **PostgreSQL** | localhost:5432 | admin / adminpassword |

### ۳. اجرای پایپ‌لاین (مرحله به مرحله)

```bash
# فاز ۰: پیش‌پردازش و EDA
docker exec -it spark_master_lakehouse spark-submit /app/scripts/00_eda_and_preprocessing.py

# فاز ۱: راه‌اندازی Delta Lake
docker exec -it spark_master_lakehouse spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /app/scripts/01_lakehouse_minio_delta.py

# فاز ۲: تولید جریان داده در کافکا (۱۰ TPS)
docker exec -it spark_master_lakehouse python3 /app/scripts/02_kafka_producer.py --tps 10 --max_records 1000

# فاز ۳: پردازش استریمینگ (۱۲۰ ثانیه)
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/03_spark_structured_streaming.py --stop_after 120

# فاز ۴: آموزش مدل
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/04_ml_train_random_forest.py --mode train

# فاز ۵: ارزیابی عملکرد
docker exec -it spark_master_lakehouse python3 /app/scripts/05_evaluation_metrics.py

# فاز ۶: انتقال داده به PostgreSQL
docker exec -it spark_master_lakehouse spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
  /app/scripts/export_delta_to_postgres.py

# فاز ۶: کوئری‌گیری مستقیم (Direct Access)
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/06_lakehouse_visualization.py
```

---

## 🔍 دسترسی مستقیم (Direct Access) – وعده‌ی Lakehouse

یکی از مهم‌ترین دستاوردهای این پروژه، **دسترسی مستقیم (Direct Access)** به داده‌های Delta Lake از طریق Spark SQL است، بدون نیاز به هیچ ETL یا دیتابیس واسط.

```sql
-- کوئری مستقیم روی Delta Lake در MinIO
SELECT
    window_start,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) as fraud_count,
    AVG(Amount) as avg_amount
FROM transactions
GROUP BY window_start
ORDER BY window_start DESC
LIMIT 10;
```

این قابلیت، **کهنگی داده (Data Staleness)** را به‌طور کامل حذف کرده و پیچیدگی زیرساخت را به‌شدت کاهش می‌دهد.

---

## 🔧 چالش‌های حل‌شده در طول توسعه

| چالش | راه‌حل |
|------|--------|
| **تصویر Bitnami Spark در دسترس نبود** | استفاده از میرور `docker.xuanyuan.run/bitnamilegacy/spark` |
| **JARهای Delta Lake پیدا نشد (`ClassNotFoundException`)** | نصب JARها از طریق `SPARK_EXTRA_JARS` و mount کردن آن‌ها |
| **خطای `ModuleNotFoundError: No module named 'superset'` در Superset** | حذف volume `.:/app` که فایل‌های اصلی Superset را بازنویسی می‌کرد |
| **خطای `column "amount" does not exist` در PostgreSQL** | استفاده از نام دقیق با کوتیشن `"Amount"` و تغییر نوع ستون‌ها |
| **خطر OOM در استریمینگ (انباشت State)** | تنظیم `withWatermark("event_timestamp", "10 seconds")` برای پاکسازی خودکار |
| **نسخه‌های ناسازگار MLflow SDK** | پیاده‌سازی Fallback از طریق REST API |
| **خطای `No module named 'py4j'` در اجرای اسکریپت‌های Python** | افزودن پویای مسیرهای PySpark و `py4j-*.zip` به `sys.path` |

---

## 📈 خلاصه‌ی فازهای پایپ‌لاین

| فاز | اسکریپت | خروجی کلیدی |
|-----|---------|-------------|
| **۰** | `00_eda_and_preprocessing.py` | داده‌های تمیز، مقیاس‌شده و تقسیم‌بندی Stratified |
| **۱** | `01_lakehouse_minio_delta.py` | جدول Delta Lake با قابلیت ACID، Time Travel، Schema Enforcement |
| **۲** | `02_kafka_producer.py` | جریان داده‌ی بلادرنگ با نرخ ۱۰/۴۰ TPS و Timestamp واقعی |
| **۳** | `03_spark_structured_streaming.py` | ویژگی‌های پنجره‌ای (تعداد، مجموع، میانگین) با Watermark و Checkpoint |
| **۴** | `04_ml_train_random_forest.py` | مدل Random Forest آموزش‌دیده، ثبت‌شده در MLflow، و استنتاج بلادرنگ |
| **۵** | `05_evaluation_metrics.py` | مقایسه‌ی تأخیر و معیارهای ML با مقاله‌ی Baseline |
| **۶** | `06_lakehouse_visualization.py` + `export_delta_to_postgres.py` | داشبورد Superset + کوئری‌های Direct Access |

---

## 👥 مشارکت‌کنندگان

**Supervisor:** Dr. Mohammad Ali Nematbakhsh, University of Isfahan

---

## 📜 لایسنس

این پروژه تحت لایسنس **MIT** منتشر شده است. برای اطلاعات بیشتر، فایل `LICENSE` را مشاهده کنید.

---

## 📬 ارتباط با ما

برای سوالات، گزارش مشکلات یا همکاری:

- **Email:** hoss3inmostaj3ran@gmail.com | hobinazire@yahoo.com
- **GitHub:** [@HosseinMst81](https://github.com/HosseinMst81)

---

<div align="center">
  <b>⭐ اگر این پروژه برای شما مفید بود، لطفاً به مخزن ستاره دهید! ⭐</b>
  <br><br>
  <i>ساخته شده با ❤️ برای جامعه‌ی Big Data</i>
</div>