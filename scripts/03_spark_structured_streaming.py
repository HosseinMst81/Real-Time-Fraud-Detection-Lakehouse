"""
===============================================================================
Phase 3: Spark Structured Streaming & Watermarked Window Processing
Real-Time Fraud Detection Lakehouse Pipeline
===============================================================================

This script implements the core Spark Structured Streaming pipeline:
1. Ingests raw JSON transaction events from Kafka topic ('creditcard-transactions').
2. Parses message payloads matching Schema Registry structure.
3. Applies Watermark on 'event_timestamp' (10-second threshold) to handle late-arriving events
   and prevent State Store memory leaks (OOM errors).
4. Computes Real-Time Windowed Feature Engineering (sliding 1-minute window, 30-second slide)
   calculating transaction velocity, aggregate amount, and risk indicators.
5. Writes processed streaming dataframe to Delta Lake storage on MinIO with Checkpointing
   for Exactly-Once fault-tolerance.
===============================================================================
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, avg, lit, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def build_spark_streaming_session(minio_endpoint: str = None, app_name: str = "FraudStructuredStreaming") -> SparkSession:
    """
    Initialize PySpark session with Kafka integration packages and Delta Lake S3A configs.
    Auto-detects Docker environment for MinIO endpoints.
    """
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    if not minio_endpoint:
        minio_endpoint = "http://minio:9000" if is_docker else os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

    print(f"[INFO] Initializing Spark Streaming Session (MinIO Endpoint: {minio_endpoint})...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.master", "local[*]") \
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_transaction_schema() -> StructType:
    """
    Define strict Schema for parsing incoming Kafka JSON payloads.
    Includes PCA features V1-V28, Time, Amount, Class, and injected event_timestamp.
    """
    fields = [
        StructField("event_timestamp", StringType(), True),
        StructField("Time", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True)
    ]
    # Add V1 through V28
    for i in range(1, 29):
        fields.append(StructField(f"V{i}", DoubleType(), True))
        
    return StructType(fields)

def execute_streaming_pipeline(
    kafka_bootstrap: str = None,
    kafka_topic: str = "creditcard-transactions",
    minio_endpoint: str = None,
    delta_sink_path: str = "s3a://lakehouse-fraud/streaming_processed",
    checkpoint_path: str = "s3a://lakehouse-fraud/checkpoints/streaming_processed",
    stop_after_seconds: int = 0,
    report: bool = True
):
    """
    Connects to Kafka, applies Watermarking & Windowed Aggregation, and streams to Delta Lake.
    """
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    if not kafka_bootstrap:
        kafka_bootstrap = "kafka:9092" if is_docker else "localhost:9092"

    spark = build_spark_streaming_session(minio_endpoint=minio_endpoint)
    schema = define_transaction_schema()
    
    print("\n" + "="*80)
    print(" STARTING SPARK STRUCTURED STREAMING PIPELINE (PHASE 3)")
    print(f" Kafka Source:       {kafka_bootstrap} | Topic: {kafka_topic}")
    print(f" Delta Sink Path:    {delta_sink_path}")
    print(f" Checkpoint Path:    {checkpoint_path}")
    print(f" Watermark Strategy: 10 seconds on event_timestamp")
    print(f" Window Spec:        Sliding 1 Minute Window (30s Slide)")
    print("="*80 + "\n")
    
    start_time = time.time()
    
    # 1. Read Raw Stream from Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        
    # 2. Deserialize JSON payload and parse ISO-8601 Timestamp
    parsed_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("parsed_event_time", to_timestamp(col("event_timestamp")))
        
    # 3. Apply Watermarking (10 seconds threshold on event_timestamp)
    #    Watermark drops state for transactions arriving > 10 seconds past the current watermark event time.
    watermarked_df = parsed_df \
        .withWatermark("parsed_event_time", "10 seconds")
        
    # 4. Real-time Feature Engineering (1-minute window, 30-second sliding step)
    windowed_features_df = watermarked_df \
        .groupBy(
            window(col("parsed_event_time"), "1 minute", "30 seconds"),
            col("Class")
        ) \
        .agg(
            count("*").alias("tx_count_1m"),
            _sum("Amount").alias("total_amount_1m"),
            avg("Amount").alias("avg_amount_1m")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("Class"),
            col("tx_count_1m"),
            col("total_amount_1m"),
            col("avg_amount_1m")
        )
        
    print("[INFO] Starting Streaming Query with OutputMode='append' & Delta Sink...")
    
    # 5. Write Stream to Delta Lake with Checkpointing (Guaranteeing Exactly-Once Fault Tolerance)
    query = windowed_features_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="5 seconds") \
        .start(delta_sink_path)
        
    print(f"[SUCCESS] Streaming Query Active (Query ID: {query.id})")
    
    try:
        if stop_after_seconds > 0:
            print(f"[INFO] Job configured to run for {stop_after_seconds} seconds before graceful shutdown...")
            time.sleep(stop_after_seconds)
            print("[INFO] Stopping query gracefully...")
            query.stop()
        else:
            print("Streaming job is running. Press Ctrl+C to stop...\n")
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Streaming job termination initiated by user.")
        query.stop()
    finally:
        total_time = time.time() - start_time
        
        # Save summary report
        summary_info = {
            "phase": "Phase 3 - Stream Processing (Spark Structured Streaming)",
            "kafka_bootstrap": kafka_bootstrap,
            "kafka_topic": kafka_topic,
            "delta_sink_path": delta_sink_path,
            "checkpoint_path": checkpoint_path,
            "watermark_spec": "10 seconds on event_timestamp",
            "window_spec": "Sliding 1 Minute Window (30s Slide)",
            "output_mode": "append",
            "duration_seconds": round(total_time, 2),
            "status": "SUCCESS",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        output_dir = "./data_output"
        os.makedirs(output_dir, exist_ok=True)
        summary_path = os.path.join(output_dir, "phase3_streaming_summary.json")
        root_summary_path = "./data_output/phase3_summary.json"
        log_path = os.path.join(output_dir, "phase3_streaming.log")
        
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_info, f, indent=2)
            
        with open(root_summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_info, f, indent=2)
            
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{summary_info['timestamp']}] Topic: {kafka_topic} | Sink: {delta_sink_path} | Status: SUCCESS\n")
            
        print("\n" + "="*80)
        print(" SPARK STRUCTURED STREAMING JOB COMPLETED / STOPPED")
        print(f" Saved Summary (Volume): {summary_path}")
        print(f" Saved Summary (Root):   {root_summary_path}")
        print(f" Saved Log:              {log_path}")
        print("="*80 + "\n")
        spark.stop()

def main():
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    default_bootstrap = "kafka:9092" if is_docker else "localhost:9092"
    default_minio = "http://minio:9000" if is_docker else "http://localhost:9000"

    parser = argparse.ArgumentParser(description="Spark Structured Streaming with Watermarked Window Aggregations")
    parser.add_argument("--kafka", type=str, default=default_bootstrap, help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="creditcard-transactions", help="Kafka input topic")
    parser.add_argument("--minio_endpoint", type=str, default=default_minio, help="MinIO S3A endpoint")
    parser.add_argument("--delta_path", type=str, default="s3a://lakehouse-fraud/streaming_processed", help="Delta Lake destination path")
    parser.add_argument("--checkpoint_path", type=str, default="s3a://lakehouse-fraud/checkpoints/streaming_processed", help="Checkpoint directory path")
    parser.add_argument("--stop_after", type=int, default=0, help="Automatically stop streaming job after N seconds (0 for infinite)")
    parser.add_argument("--report", action="store_true", help="Generate report files")
    
    args = parser.parse_args()
    
    execute_streaming_pipeline(
        kafka_bootstrap=args.kafka,
        kafka_topic=args.topic,
        minio_endpoint=args.minio_endpoint,
        delta_sink_path=args.delta_path,
        checkpoint_path=args.checkpoint_path,
        stop_after_seconds=args.stop_after,
        report=args.report
    )

if __name__ == "__main__":
    main()

