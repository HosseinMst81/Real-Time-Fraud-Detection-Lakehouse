# Project Structure

Generated on: 8/2/2026, 1:15:39 PM
Root: d:\Daneshgoh\Master\Term 2\Big Data\Final Project\Real-Time Fraud Detection Lakehouse
Excluded: .git, docs, jars, metastore_db, venv

```
├── data_output/
│   ├── fraud_lakehouse.db
│   ├── phase0_summary.json
│   ├── phase1_execution_report.txt
│   ├── phase2_kafka_producer_summary.json
│   ├── phase2_summary.json
│   ├── phase3_streaming_summary.json
│   ├── phase4_ml_training_summary.json
│   ├── phase5_evaluation_summary.json
│   ├── phase6_visualization_summary.json
│   ├── test_split.csv
│   └── train_split.csv
├── scripts/
│   ├── __pycache__/
│   │   ├── 00_eda_and_preprocessing.cpython-312.pyc
│   │   ├── 01_lakehouse_minio_delta.cpython-312.pyc
│   │   ├── 02_kafka_producer.cpython-312.pyc
│   │   ├── 02_kafka_producer.cpython-314.pyc
│   │   ├── 03_spark_structured_streaming.cpython-312.pyc
│   │   ├── 04_ml_train_random_forest.cpython-312.pyc
│   │   └── 05_evaluation_metrics.cpython-312.pyc
│   ├── 00_eda_and_preprocessing.py
│   ├── 01_lakehouse_minio_delta.py
│   ├── 02_kafka_producer.py
│   ├── 03_spark_structured_streaming.py
│   ├── 04_ml_train_random_forest.py
│   ├── 05_evaluation_metrics.py
│   ├── 06_lakehouse_visualization.py
│   └── export_delta_to_postgres.py
├── spark-warehouse/
│   └── fraud_lakehouse.db/
├── sql/
│   ├── widget-01.sql
│   ├── widget-02.sql
│   ├── widget-03.sql
│   └── widget-04.sql
├── .gitignore
├── creditcard.csv
├── docker-compose.yml
├── Final_report.md
├── requirements.txt
└── superset_config.py
```