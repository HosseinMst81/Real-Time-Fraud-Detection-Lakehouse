# 🏦 Real-Time Fraud Detection Lakehouse Pipeline

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

[📖 **Read this in Persian**](./README.fa.md) | [📖 **English Version**](./README.md)

---

## 📌 Overview

This project implements a **production-grade, end-to-end real-time fraud detection pipeline** built on a **Lakehouse architecture**. It unifies **streaming data ingestion**, **machine learning**, and **interactive analytics** on a single open platform using **Delta Lake**, **Apache Spark**, **Kafka**, and **MLflow**.

> 🎯 **Key Achievement**: Achieved **~47% lower latency** compared to traditional two-tier architectures, while delivering **81% precision** and **86% recall** on a highly imbalanced credit card fraud dataset (fraud ratio: 0.172%).

Developed as the **final project** for the **Big Data Processing** graduate course at the University of Isfahan, this system is capable of ingesting thousands of transactions per second in real-time, applying intelligent ML-based fraud detection, and making results available for visualization with **zero data staleness** — all without complex ETL pipelines.

---

## 🏗️ Architecture & Data Flow

The pipeline follows a **Stream-to-Lakehouse** pattern. All components are fully **containerized** and orchestrated via Docker Compose.

```
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

## 🚀 Key Features

### 🧠 Machine Learning
- **Random Forest Classifier** with 100 trees, max depth 10
- **Cost-Sensitive Learning** (class weight = 290.0) to handle extreme class imbalance (0.172% fraud)
- **MLflow integration** with dual logging (SDK + REST API fallback for reliability)
- **Model Registry** for versioning and production deployment
- **Real-time streaming inference** with fraud alerts published to Kafka

### ⚡ Streaming Pipeline
- **Apache Kafka** with KRaft mode (no ZooKeeper dependency)
- **Spark Structured Streaming** with exactly-once semantics
- **Watermark** (10 seconds) to prevent OOM and manage late-arriving data
- **Sliding Window** (1-minute duration, 30-second slide) for real-time feature engineering
- **Checkpointing** for fault tolerance and state recovery

### 🏛️ Lakehouse Storage
- **Delta Lake** on **MinIO** (S3-compatible object storage)
- **ACID transactions** with automatic rollback on failure
- **Time Travel** (versioned snapshots for audit and rollback)
- **Schema Enforcement** and **Schema Evolution** (mergeSchema)
- **Z-ORDER indexing** and **VACUUM** for query performance and storage optimization
- **Direct Access** – query Delta Lake files directly via Spark SQL (no ETL required)

### 📊 Visualization & Monitoring
- **Apache Superset** dashboard with 4 key widgets:
  1. **Legitimate vs Fraudulent Transactions per Minute** (Bar Chart)
  2. **Total Fraudulent Amount ($)** – KPI Big Number with Trendline
  3. **End-to-End Processing Latency Trends** (Line Chart)
  4. **Sliding Window Fraud Rate (%)** (Area/Line Chart)
- **Direct Access** – run Spark SQL queries directly on Delta Lake without intermediate databases

---

## 🛠️ Technology Stack

| Layer | Technology | Version |
|-------|------------|---------|
| **Processing Engine** | Apache Spark | 3.5.0 |
| **Storage Format** | Delta Lake | 3.1.0 |
| **Streaming Bus** | Apache Kafka (KRaft) | 7.6.0 |
| **Object Storage** | MinIO | Latest |
| **Model Registry** | MLflow | 2.10.2 |
| **Visualization** | Apache Superset | 3.0.2 |
| **Relational Database** | PostgreSQL | 15-alpine |
| **Orchestration** | Docker Compose | 3.8 |
| **Language** | Python | 3.12+ |

---

## 📁 Project Structure

```
Real-Time-Fraud-Detection-Lakehouse/
├── scripts/
│   ├── 00_eda_and_preprocessing.py          # Phase 0: EDA, scaling & stratified split
│   ├── 01_lakehouse_minio_delta.py          # Phase 1: Delta Lake setup on MinIO
│   ├── 02_kafka_producer.py                 # Phase 2: Kafka streaming with configurable TPS
│   ├── 03_spark_structured_streaming.py     # Phase 3: Watermark, window & sink to Delta
│   ├── 04_ml_train_random_forest.py         # Phase 4: ML training & inference with MLflow
│   ├── 05_evaluation_metrics.py             # Phase 5: Latency & ML benchmark comparison
│   ├── 06_lakehouse_visualization.py        # Phase 6: Direct Access & query engine
│   └── export_delta_to_postgres.py          # Sync Delta Lake to PostgreSQL for Superset
├── sql/
│   ├── widget-01.sql                        # Superset chart queries
│   ├── widget-02.sql
│   ├── widget-03.sql
│   └── widget-04.sql
├── data_output/                             # Generated outputs (CSVs, JSON summaries)
├── docker-compose.yml                       # Full container orchestration
├── requirements.txt                         # Python dependencies
├── superset_config.py                       # Superset custom configuration
├── .gitignore
└── README.md
```

---

## 📊 Performance Results

### 1. Model Accuracy Metrics (vs. Baseline Paper)

| Metric | Baseline Paper | Our Pipeline | Improvement |
|--------|---------------|--------------|-------------|
| **Precision** | 0.34 | **0.81** | **+138%** |
| **Recall**   | 0.88 | **0.86** | Maintained high |
| **F1-Score** | 0.49 | **0.83** | **+69%** |
| **ROC-AUC**  | 0.94 | **0.96** | **+2%** |

> **Why the jump in Precision?** We used **Cost-Sensitive Learning** with a class weight of 290.0 (instead of artificial oversampling). This drastically reduced false positives while keeping recall high – a critical trade-off in fraud detection where false alarms are costly.

### 2. End-to-End Latency Benchmarks

| Throughput | Baseline Paper | Our Pipeline | Improvement |
|------------|---------------|--------------|-------------|
| **10 TPS** (normal load) | 0.80s | **0.42s** | **47.5% faster** |
| **40 TPS** (peak load)   | 0.90s | **0.58s** | **35.6% faster** |

> **Why faster?** Direct Access eliminates ETL bottlenecks, Delta Lake’s optimized Parquet format reduces I/O, and containerized Spark leverages better resource isolation.

---

## 🚀 Quick Start Guide

### Prerequisites
- Docker Desktop (or Docker Engine + Compose)
- Python 3.12+ (for local development)
- Git

### 1. Clone & Setup

```bash
git clone https://github.com/yourusername/Real-Time-Fraud-Detection-Lakehouse.git
cd Real-Time-Fraud-Detection-Lakehouse

# Create virtual environment
python -m venv venv
source venv/Scripts/activate      # Windows: venv\Scripts\activate

# Install dependencies (use mirror if needed)
pip install -r requirements.txt -i https://pypi.iranrepo.ir/simple
```

### 2. Launch All Services

```bash
docker compose up -d
```

Verify all containers are running:
```bash
docker compose ps
```

**Service Access URLs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadminpassword |
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark Worker UI** | http://localhost:8082 | - |
| **Kafka Broker** | localhost:9092 | - |
| **Schema Registry** | http://localhost:8081 | - |
| **MLflow UI** | http://localhost:5000 | - |
| **Apache Superset** | http://localhost:8088 | admin / admin |
| **Apache NiFi** | https://localhost:8443 | admin / nifipassword123456 |
| **PostgreSQL** | localhost:5432 | admin / adminpassword |

### 3. Run the Pipeline (Step by Step)

```bash
# Phase 0: EDA & Preprocessing
docker exec -it spark_master_lakehouse spark-submit /app/scripts/00_eda_and_preprocessing.py

# Phase 1: Delta Lake Setup on MinIO
docker exec -it spark_master_lakehouse spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /app/scripts/01_lakehouse_minio_delta.py

# Phase 2: Kafka Producer (10 TPS)
docker exec -it spark_master_lakehouse python3 /app/scripts/02_kafka_producer.py --tps 10 --max_records 1000

# Phase 3: Spark Streaming (runs for 120 seconds, then stops)
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/03_spark_structured_streaming.py --stop_after 120

# Phase 4: ML Training
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/04_ml_train_random_forest.py --mode train

# Phase 5: Evaluation
docker exec -it spark_master_lakehouse python3 /app/scripts/05_evaluation_metrics.py

# Phase 6: Export to PostgreSQL & Visualize
docker exec -it spark_master_lakehouse spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
  /app/scripts/export_delta_to_postgres.py

# Phase 6: Direct Access Query Engine
docker exec -it spark_master_lakehouse spark-submit \
  /app/scripts/06_lakehouse_visualization.py
```

---

## 🔍 Direct Access – The Lakehouse Promise

One of the key differentiators of this project is **Direct Access** – the ability to query Delta Lake files directly using Spark SQL without any ETL to a separate database.

```sql
-- Query Delta Lake directly on MinIO
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

This eliminates **data staleness** (zero-lag analytics), reduces infrastructure complexity, and enables real-time decision-making.

---

## 🔧 Troubleshooting & Resolved Challenges

| Challenge | Solution |
|-----------|----------|
| **Bitnami Spark image unavailable** | Used legacy mirror `docker.xuanyuan.run/bitnamilegacy/spark` |
| **Delta Lake JARs not found (`ClassNotFoundException`)** | Mounted JARs via `SPARK_EXTRA_JARS` and Docker volume |
| **Superset `ModuleNotFoundError: No module named 'superset'`** | Removed `.:/app` volume that was overwriting core Superset files |
| **PostgreSQL `column "amount" does not exist`** | Used case-sensitive quoting `"Amount"` and altered column types |
| **OOM risk in streaming (State Store growth)** | Configured `withWatermark("event_timestamp", "10 seconds")` to drop old states |
| **MLflow SDK version conflicts** | Implemented REST API fallback for model logging/registration |
| **Py4J `ModuleNotFoundError` when running Python scripts** | Injected PySpark paths and `py4j-*.zip` into `sys.path` dynamically |

---

## 📈 Pipeline Phases at a Glance

| Phase | Script | Key Deliverable |
|-------|--------|-----------------|
| **0** | `00_eda_and_preprocessing.py` | Clean, scaled, stratified train/val/test splits |
| **1** | `01_lakehouse_minio_delta.py` | Delta Lake table with ACID, Time Travel, Schema Enforcement |
| **2** | `02_kafka_producer.py` | Real-time data stream at 10/40 TPS with wall-clock timestamps |
| **3** | `03_spark_structured_streaming.py` | Windowed features (count, sum, avg) with watermark & checkpointing |
| **4** | `04_ml_train_random_forest.py` | Trained Random Forest model logged in MLflow, real-time inference |
| **5** | `05_evaluation_metrics.py` | Latency benchmarks & ML metric comparison vs. baseline |
| **6** | `06_lakehouse_visualization.py` + `export_delta_to_postgres.py` | Superset dashboard + Direct Access SQL queries |

---

## 📚 References

1. Armbrust, M., et al. (2020). "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores." *PVLDB*, 13(12).
2. Armbrust, M., et al. (2018). "Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark." *SIGMOD '18*.
3. Behm, A., et al. (2022). "Photon: A Vectorized Query Engine for Lakehouse Workloads." *SIGMOD '22*.
4. Dal Pozzolo, A., et al. (2015). "Calibrating Probability with Undersampling for Unbalanced Classification." *IEEE SSCI*.
5. Carcillo, F., et al. (2018). "Scarff: A Scalable Framework for Streaming Credit Card Fraud Detection with Spark." *Information Systems*, 78.

---

## 👥 Contributors

**Supervisor:** Dr. Mohammad Ali Nematbakhsh, University of Isfahan

---

## 📜 License

Distributed under the **MIT License**. See the `LICENSE` file for more information.

---

## 📬 Contact

For questions, issues, or collaboration:
- **Email:** hoss3inmostaj3ran@gmail.com | hobinazire@yahoo.com
- **GitHub:** [@HosseinMst81](https://github.com/HosseinMst81)

---

<div align="center">
  <b>⭐ If this project helped you, please star the repository! ⭐</b>
  <br><br>
  <i>Built with ❤️ for the Big Data community</i>
</div>