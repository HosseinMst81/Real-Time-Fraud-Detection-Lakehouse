"""
===============================================================================
Phase 2: High-Throughput Kafka Producer with Wall-Clock Timestamping
Real-Time Fraud Detection Lakehouse Pipeline
===============================================================================
Author: Senior Big Data Specialist

This script reads preprocessed credit card transaction records and streams them to an
Apache Kafka topic ('creditcard-transactions') at a controlled rate (e.g., 10 TPS or 40 TPS).

Key Requirements Implemented:
1. Configurable TPS rate (10 TPS and 40 TPS) matching benchmark scenarios.
2. Injects real wall-clock ISO-8601 timestamp ('event_timestamp') into each JSON record.
   (Required because original dataset 'Time' column is relative seconds, not a wall-clock timestamp).
3. Schema compliance matching Confluent Schema Registry specifications.
===============================================================================
"""

import os
import sys
import time
import json
import argparse
import subprocess
import importlib.util
from datetime import datetime, timezone

# Ensure required libraries are installed in current Python environment
def ensure_dependencies():
    missing = []
    try:
        import kafka
    except ImportError:
        missing.append("kafka-python")
    try:
        import requests
    except ImportError:
        missing.append("requests")
    try:
        import pandas
    except ImportError:
        missing.append("pandas")
        
    if missing:
        print(f"[INFO] Installing missing Python packages: {missing}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        except Exception as e:
            print(f"[WARNING] Could not auto-install packages via pip: {e}")

ensure_dependencies()

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import requests
except ImportError:
    requests = None

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None

def load_preprocessing_dataset(csv_path: str):
    """
    Load load_dataset() from the phase-0 script.

    The phase file starts with digits, so it cannot be imported with normal
    dotted import syntax.
    """
    module_path = os.path.join(os.path.dirname(__file__), "00_eda_and_preprocessing.py")
    spec = importlib.util.spec_from_file_location("phase0_eda_and_preprocessing", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load preprocessing module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_dataset(csv_path)

def register_schema_registry(schema_registry_url: str, subject_name: str):
    """
    Register JSON Schema into Confluent Schema Registry to enforce schema validation.
    Links directly to Schema Enforcement concept in Lakehouse & Streaming papers.
    """
    url = f"{schema_registry_url.rstrip('/')}/subjects/{subject_name}/versions"
    
    json_schema = {
        "schemaType": "JSON",
        "schema": json.dumps({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "CreditCardTransactionEvent",
            "type": "object",
            "properties": {
                "event_timestamp": {"type": "string"},
                "Time": {"type": "number"},
                "Amount": {"type": "number"},
                "Class": {"type": "integer"}
            },
            "required": ["event_timestamp", "Time", "Amount", "Class"]
        })
    }
    
    data_bytes = json.dumps(json_schema).encode("utf-8")
    
    if requests is not None:
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        try:
            response = requests.post(url, data=json.dumps(json_schema), headers=headers, timeout=5)
            if response.status_code in (200, 201):
                print(f"[SUCCESS] Registered schema in Schema Registry: {subject_name} (ID: {response.json().get('id')})")
            else:
                print(f"[INFO] Schema Registry status ({response.status_code}): {response.text}")
        except Exception as ex:
            print(f"[NOTE] Schema Registry offline or unreachable ({url}): {ex}")
    else:
        # Fallback using urllib.request
        import urllib.request
        try:
            req = urllib.request.Request(url, data=data_bytes, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, method="POST")
            with urllib.request.urlopen(req, timeout=5) as resp:
                print(f"[SUCCESS] Registered schema in Schema Registry via urllib: {subject_name}")
        except Exception as ex:
            print(f"[NOTE] Schema Registry offline or unreachable ({url}): {ex}")

# Sample JSON payload structure requirement
"""
{
  "event_timestamp": "2026-07-24T10:23:45.123456Z",
  "Time": 406.0,
  "V1": -1.35,
  "V2": 0.42,
  ...
  "V28": -0.05,
  "Amount": 149.62,
  "Class": 0
}
"""

def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Initialize Kafka Producer instance with JSON serializer and reliable acknowledgement configs.
    """
    print(f"[INFO] Connecting Kafka Producer to broker: {bootstrap_servers}")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks='all',  # Guarantee durable delivery
        retries=5,
        max_in_flight_requests_per_connection=1
    )
    return producer

def stream_transactions_to_kafka(
    dataset_path: str,
    topic_name: str,
    target_tps: float,
    bootstrap_servers: str,
    schema_registry_url: str = None,
    max_records: int = None
):
    """
    Reads dataset rows and streams to Kafka topic at exact target_tps rate.
    Injects ISO-8601 wall-clock timestamp 'event_timestamp'.
    """
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    
    if not schema_registry_url:
        schema_registry_url = "http://schema-registry:8081" if is_docker else "http://localhost:8081"

    # Attempt Schema Registry subject registration
    register_schema_registry(schema_registry_url, f"{topic_name}-value")

    records = []
    if not os.path.exists(dataset_path):
        print(f"[WARNING] Dataset file '{dataset_path}' missing. Generating dynamic stream records...")
        if pd is not None:
            df = load_preprocessing_dataset(dataset_path)
            records = df.to_dict(orient="records")
        else:
            # Synthetic row generator if dataset missing & pandas not installed
            records = [{"Time": i, "Amount": 100.0, "Class": 0} for i in range(max_records or 100)]
    else:
        if pd is not None:
            df = pd.read_csv(dataset_path)
            records = df.to_dict(orient="records")
        else:
            import csv
            print(f"[INFO] Reading dataset via standard csv module: {dataset_path}")
            with open(dataset_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert numeric fields
                    typed_row = {}
                    for k, v in row.items():
                        try:
                            typed_row[k] = float(v) if "." in v else int(v)
                        except ValueError:
                            typed_row[k] = v
                    records.append(typed_row)
        
    producer = create_kafka_producer(bootstrap_servers)
    
    interval_per_record = 1.0 / target_tps
    total_records = len(records) if max_records is None else min(len(records), max_records)
    
    print("\n" + "="*80)
    print(f" STARTING KAFKA STREAMING PRODUCER")
    print(f" Target Topic:        {topic_name}")
    print(f" Target Throughput:   {target_tps} TPS ({interval_per_record*1000:.2f} ms / record)")
    print(f" Total Records:       {total_records:,}")
    print("="*80 + "\n")
    
    sent_count = 0
    start_time = time.time()
    
    try:
        for idx, record_dict in enumerate(records):
            if max_records and sent_count >= max_records:
                break
                
            # Inject Wall-Clock Timestamp (UTC ISO-8601 format)
            wall_clock_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            record_dict['event_timestamp'] = wall_clock_now
            
            # Key partitioning by Class or random key
            transaction_key = f"tx_{int(record_dict.get('Time', idx))}"
            
            # Async produce to Kafka
            producer.send(
                topic=topic_name,
                key=transaction_key,
                value=record_dict
            )
            
            sent_count += 1
            
            # Print status log
            if sent_count % int(max(1, target_tps * 2)) == 0 or sent_count == 1:
                elapsed = time.time() - start_time
                actual_tps = sent_count / elapsed if elapsed > 0 else 0
                print(f"[PRODUCER STATUS] Sent: {sent_count}/{total_records} | Actual Rate: {actual_tps:.2f} TPS | Event Time: {wall_clock_now}")
                
            # Precise Rate Limiting Sleep
            target_elapsed = sent_count * interval_per_record
            actual_elapsed = time.time() - start_time
            sleep_needed = target_elapsed - actual_elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)
                
    except KeyboardInterrupt:
        print("\n[INFO] Streaming producer stopped manually by user.")
    finally:
        producer.flush()
        producer.close()
        total_time = time.time() - start_time
        print("\n" + "="*80)
        print(f" PRODUCER STREAMING COMPLETED")
        print(f" Sent Records: {sent_count:,}")
        print(f" Duration:     {total_time:.2f} seconds")
        print(f" Average TPS:  {sent_count / total_time if total_time > 0 else 0:.2f} TPS")
        print("="*80 + "\n")

def main():
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    default_bootstrap = "kafka:9092" if is_docker else "localhost:9092,localhost:29092"
    default_schema_reg = "http://schema-registry:8081" if is_docker else "http://localhost:8081"

    parser = argparse.ArgumentParser(description="Kafka Streaming Producer for Credit Card Fraud Detection")
    parser.add_argument("--dataset", type=str, default="./data_output/test_split.csv", help="Path to input CSV dataset")
    parser.add_argument("--topic", type=str, default="creditcard-transactions", help="Kafka topic name")
    parser.add_argument("--tps", type=float, default=10.0, help="Transactions per second target rate (e.g. 10 or 40)")
    parser.add_argument("--bootstrap", type=str, default=default_bootstrap, help="Kafka bootstrap servers")
    parser.add_argument("--schema_registry", type=str, default=default_schema_reg, help="Confluent Schema Registry URL")
    parser.add_argument("--max_records", type=int, default=100, help="Maximum records to stream (None for all)")
    
    args = parser.parse_args()
    stream_transactions_to_kafka(
        dataset_path=args.dataset,
        topic_name=args.topic,
        target_tps=args.tps,
        bootstrap_servers=args.bootstrap,
        schema_registry_url=args.schema_registry,
        max_records=args.max_records
    )

if __name__ == "__main__":
    main()
