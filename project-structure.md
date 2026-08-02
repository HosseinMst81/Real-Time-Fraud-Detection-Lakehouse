# Project Structure

Generated on: 7/25/2026, 3:53:17 PM
Root: d:\Daneshgoh\Master\Term 2\Big Data\Final Project\Real-Time Fraud Detection Lakehouse
Excluded: .git, venv

```
├── data_output/
│   ├── phase0_summary.json
│   ├── test_split.csv
│   └── train_split.csv
├── scripts/
│   ├── 00_eda_and_preprocessing.py
│   ├── 01_lakehouse_minio_delta.py
│   ├── 02_kafka_producer.py
│   ├── 03_spark_structured_streaming.py
│   ├── 04_ml_train_random_forest.py
│   └── 05_evaluation_metrics.py
├── .gitignore
├── creditcard.csv
├── docker-compose.yml
└── requirements.txt
```

docker exec -it spark_master_lakehouse spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/scripts/01_lakehouse_minio_delta.py

: Script 06
docker exec -it spark_master_lakehouse spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/scripts/06_lakehouse_visualization.py

superset setup:
docker exec -it spark_master_lakehouse spark-submit 
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 
  /app/scripts/export_delta_to_postgres.py

set superset pass:
docker exec -it superset_lakehouse superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin


1. docker exec -it superset_lakehouse superset db upgrade
2. docker exec -it superset_lakehouse superset init
3. docker exec -it superset_lakehouse superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin


export data to postgresql:
docker exec -it spark_master_lakehouse spark-submit
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0
  /app/scripts/export_delta_to_postgres.py

connect to postgreSQL container:
docker exec -it postgres_lakehouse psql -U admin -d fraud_lakehouse

changne column name: 
ALTER TABLE transactions 
ALTER COLUMN window_start TYPE TIMESTAMP 
USING window_start::TIMESTAMP;

Approve Changes:
\d transactions

Quit:
\q