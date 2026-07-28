"""
===============================================================================
Phase 4: Spark MLlib Model Training & Real-Time Streaming Inference with MLflow
Real-Time Fraud Detection Lakehouse Pipeline
===============================================================================

This script handles Machine Learning training and streaming inference:
1. Trains a Spark MLlib RandomForestClassifier on preprocessed transaction features.
2. Implements Undersampling to handle severe class imbalance (~0.172% fraud rate).
3. Evaluates model using BinaryClassificationEvaluator & MulticlassClassificationEvaluator (ROC-AUC, PR-AUC, Precision, Recall, F1).
4. Logs parameters, metrics, and registers the model into MLflow Tracking Server / Model Registry.
5. Exports trained PipelineModel to 's3a://lakehouse-fraud/models/rf_fraud_model'.
6. Applies model to Spark Structured Streaming micro-batches and publishes detected fraud
   alerts (prediction == 1.0) to Kafka topic ('fraud-alerts').
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

def ensure_dependencies():
    try:
        import mlflow
    except ImportError:
        print("[INFO] Attempting auto-installation of mlflow with --no-deps to bypass pydantic version conflicts...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow", "--no-deps"])
        except Exception as e:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
            except Exception as e2:
                print(f"[WARNING] Could not auto-install mlflow: {e2}")

ensure_dependencies()

try:
    import mlflow
    import mlflow.spark
except ImportError:
    mlflow = None

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_json, struct, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def log_to_mlflow_rest(mlflow_uri: str, params: dict, metrics: dict, model_path: str):
    """
    Direct REST API logger for MLflow Tracking Server.
    Communicates via standard HTTP REST API, bypassing python mlflow SDK dependency issues.
    """
    import urllib.request
    import urllib.error
    
    base_url = mlflow_uri.rstrip('/')
    headers = {"Content-Type": "application/json"}
    
    def post(endpoint: str, data: dict):
        url = f"{base_url}{endpoint}"
        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            return {"error": str(e)}

    # 1. Get or Create Experiment
    exp_name = "Credit_Card_Fraud_Detection_Lakehouse"
    exp_res = post("/api/2.0/mlflow/experiments/get-by-name", {"experiment_name": exp_name})
    exp_id = None
    if "experiment" in exp_res and "experiment_id" in exp_res["experiment"]:
        exp_id = exp_res["experiment"]["experiment_id"]
    else:
        create_exp = post("/api/2.0/mlflow/experiments/create", {"name": exp_name})
        exp_id = create_exp.get("experiment_id")

    if not exp_id:
        print(f"[WARNING] MLflow REST API: Could not create or locate experiment '{exp_name}' at {mlflow_uri}")
        return False

    # 2. Create Run
    now_ms = int(time.time() * 1000)
    run_res = post("/api/2.0/mlflow/runs/create", {
        "experiment_id": exp_id,
        "start_time": now_ms,
        "run_name": "Spark_MLlib_RandomForest_Undersampled",
        "tags": [{"key": "mlflow.source.name", "value": "04_ml_train_random_forest.py"}]
    })
    run_info = run_res.get("run", {}).get("info", {})
    run_id = run_info.get("run_id")
    if not run_id:
        print(f"[WARNING] MLflow REST API: Could not create run. Response: {run_res}")
        return False

    # 3. Log Parameters and Metrics
    param_list = [{"key": str(k), "value": str(v)} for k, v in params.items()]
    metric_list = [{"key": str(k), "value": float(v), "timestamp": now_ms, "step": 0} for k, v in metrics.items()]
    
    post("/api/2.0/mlflow/runs/log-batch", {
        "run_id": run_id,
        "params": param_list,
        "metrics": metric_list
    })

    # 4. Finish Run
    post("/api/2.0/mlflow/runs/update", {
        "run_id": run_id,
        "status": "FINISHED",
        "end_time": int(time.time() * 1000)
    })

    # 5. Register Model in Model Registry
    reg_name = "FraudDetectionRandomForest"
    post("/api/2.0/mlflow/registered-models/create", {"name": reg_name})
    post("/api/2.0/mlflow/model-versions/create", {
        "name": reg_name,
        "source": model_path,
        "run_id": run_id
    })

    print(f"[SUCCESS] Registered Run and Model in MLflow UI via REST API! (Experiment ID: {exp_id}, Run ID: {run_id})")
    return True

def create_ml_spark_session(minio_endpoint: str = None, app_name: str = "FraudMLlibTraining") -> SparkSession:
    """
    Initialize Spark Session with MLlib, Delta Lake, Kafka integration, and MinIO S3A configs.
    """
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    if not minio_endpoint:
        minio_endpoint = "http://minio:9000" if is_docker else os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")

    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

    print(f"[INFO] Initializing Spark MLlib Session (MinIO Endpoint: {minio_endpoint})...")

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
    """Returns explicit schema matching credit card dataset payload."""
    fields = [
        StructField("event_timestamp", StringType(), True),
        StructField("Time", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True)
    ]
    for i in range(1, 29):
        fields.append(StructField(f"V{i}", DoubleType(), True))
    return StructType(fields)

def train_and_register_fraud_model(
    train_csv_path: str = "./data_output/train_split.csv",
    val_csv_path: str = "./data_output/test_split.csv",
    model_save_path: str = "s3a://lakehouse-fraud/models/rf_fraud_model",
    minio_endpoint: str = None,
    num_trees: int = 100,
    max_depth: int = 10,
    use_undersampling: bool = True
):
    """
    Builds, trains, evaluates, and logs RandomForestClassifier pipeline with Undersampling.
    Logs run and registers model in MLflow Model Registry.
    """
    spark = create_ml_spark_session(minio_endpoint=minio_endpoint)
    start_time = time.time()
    
    print("\n" + "="*80)
    print(" PHASE 4: SPARK MLLIB RANDOM FOREST MODEL TRAINING & MLFLOW REGISTRATION")
    print("="*80)
    
    # Configure MLflow Tracking Server
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    default_mlflow_uri = "http://mlflow:5000" if is_docker else "http://localhost:5000"
    mlflow_uri = os.environ.get("MLFLOW_TRACKING_URI", default_mlflow_uri)
    
    mlflow_connected = False
    if mlflow is not None:
        print(f"[INFO] Setting MLflow Tracking URI: {mlflow_uri}")
        try:
            mlflow.set_tracking_uri(mlflow_uri)
            mlflow.set_experiment("Credit_Card_Fraud_Detection_Lakehouse")
            mlflow_connected = True
        except Exception as e:
            print(f"[WARNING] MLflow server unreachable ({mlflow_uri}): {e}. Continuing with local model saving...")
    else:
        print("[WARNING] MLflow module is not available. Skipping MLflow tracking/registry logging.")

    print(f"[INFO] Loading training dataset: {train_csv_path}")
    if os.path.exists(train_csv_path):
        train_df = spark.read.option("header", "true").option("inferSchema", "true").csv(train_csv_path)
        val_df = spark.read.option("header", "true").option("inferSchema", "true").csv(val_csv_path)
    else:
        print("[WARNING] Split CSVs missing. Generating synthetic balanced Spark dataset...")
        _module_path = os.path.join(os.path.dirname(__file__), "00_eda_and_preprocessing.py")
        _spec = importlib.util.spec_from_file_location("phase0_eda_and_preprocessing", _module_path)
        if _spec is None or _spec.loader is None:
            raise ImportError(f"Unable to load preprocessing module from {_module_path}")
        _module = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        df_pd = _module.load_dataset("creditcard.csv")
        spark_df = spark.createDataFrame(df_pd)
        train_df, val_df = spark_df.randomSplit([0.8, 0.2], seed=42)
        
    total_raw_count = train_df.count()
    raw_fraud_count = train_df.filter(col("Class") == 1).count()
    raw_legit_count = train_df.filter(col("Class") == 0).count()
    
    print(f"-> Raw Training Samples: {total_raw_count:,} | Legit: {raw_legit_count:,} | Fraud: {raw_fraud_count:,}")
    print(f"-> Fraud Ratio in Raw Data: {raw_fraud_count / max(total_raw_count, 1):.4%}")
    
    # Handle Class Imbalance via Undersampling (Majority Class Reduction)
    if use_undersampling and raw_fraud_count > 0 and raw_legit_count > raw_fraud_count:
        print("[INFO] Applying Random Undersampling on Majority Class (Class 0)...")
        fraction = float(raw_fraud_count) / float(raw_legit_count)
        legit_undersampled = train_df.filter(col("Class") == 0).sample(withReplacement=False, fraction=fraction, seed=42)
        fraud_subset = train_df.filter(col("Class") == 1)
        
        balanced_train_df = fraud_subset.union(legit_undersampled)
        balanced_total = balanced_train_df.count()
        balanced_fraud = balanced_train_df.filter(col("Class") == 1).count()
        balanced_legit = balanced_train_df.filter(col("Class") == 0).count()
        print(f"[SUCCESS] Undersampling Complete -> Balanced Total: {balanced_total:,} (Legit: {balanced_legit:,}, Fraud: {balanced_fraud:,})")
    else:
        balanced_train_df = train_df
        balanced_total = total_raw_count
        
    # Feature Columns (V1 to V28 + Amount + Time)
    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount", "Time"]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="Class",
        numTrees=num_trees,
        maxDepth=max_depth,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    rf_params = {
        "numTrees": num_trees,
        "maxDepth": max_depth,
        "seed": 42,
        "balancing_method": "Undersampling" if use_undersampling else "None",
        "raw_samples": total_raw_count,
        "balanced_samples": balanced_total
    }
    
    # Fit Pipeline Model
    print(f"\n[INFO] Fitting Random Forest Classifier (numTrees={num_trees}, maxDepth={max_depth})...")
    model = pipeline.fit(balanced_train_df)
    
    print("[INFO] Evaluating on Validation Dataset...")
    predictions = model.transform(val_df)
    
    # Metrics Calculation
    eval_roc = BinaryClassificationEvaluator(labelCol="Class", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(labelCol="Class", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    eval_multi = MulticlassClassificationEvaluator(labelCol="Class", predictionCol="prediction")
    
    roc_auc = float(eval_roc.evaluate(predictions))
    pr_auc = float(eval_pr.evaluate(predictions))
    
    eval_multi.setMetricName("weightedPrecision")
    precision = float(eval_multi.evaluate(predictions))
    
    eval_multi.setMetricName("weightedRecall")
    recall = float(eval_multi.evaluate(predictions))
    
    eval_multi.setMetricName("f1")
    f1 = float(eval_multi.evaluate(predictions))
    
    print("\n" + "="*80)
    print(" MODEL EVALUATION METRICS SUMMARY")
    print("="*80)
    print(f" ROC-AUC Area:  {roc_auc:.4f}")
    print(f" PR-AUC Area:   {pr_auc:.4f}")
    print(f" Precision:     {precision:.4f}")
    print(f" Recall:        {recall:.4f}")
    print(f" F1-Score:      {f1:.4f}")
    print("="*80 + "\n")
    
    # Save Pipeline Model to MinIO S3A Path
    print(f"[INFO] Exporting fitted PipelineModel to: {model_save_path}")
    model.write().overwrite().save(model_save_path)
    print("[SUCCESS] Model exported successfully.")
    
    # Log run & register model in MLflow (SDK or REST API fallback)
    metrics_dict = {
        "roc_auc": round(roc_auc, 4),
        "pr_auc": round(pr_auc, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4)
    }

    if mlflow_connected:
        try:
            with mlflow.start_run(run_name="Spark_MLlib_RandomForest_Undersampled"):
                mlflow.log_params(rf_params)
                mlflow.log_metric("roc_auc", roc_auc)
                mlflow.log_metric("pr_auc", pr_auc)
                mlflow.log_metric("precision", precision)
                mlflow.log_metric("recall", recall)
                mlflow.log_metric("f1_score", f1)
                
                # Register Model in MLflow Model Registry
                mlflow.spark.log_model(
                    model, 
                    artifact_path="spark-rf-model", 
                    registered_model_name="FraudDetectionRandomForest"
                )
                print("[SUCCESS] Model logged and registered in MLflow Model Registry as 'FraudDetectionRandomForest'")
        except Exception as ex:
            print(f"[NOTE] MLflow SDK note: {ex}. Falling back to REST API...")
            log_to_mlflow_rest(mlflow_uri, rf_params, metrics_dict, model_save_path)
    else:
        print(f"[INFO] MLflow SDK unavailable. Attempting REST API logging to MLflow Server ({mlflow_uri})...")
        log_to_mlflow_rest(mlflow_uri, rf_params, metrics_dict, model_save_path)

    total_time = time.time() - start_time
    
    summary_info = {
        "phase": "Phase 4 - Machine Learning Model Training (Spark MLlib & MLflow)",
        "model_type": "RandomForestClassifier",
        "num_trees": num_trees,
        "max_depth": max_depth,
        "balancing_method": "Random Undersampling",
        "raw_train_samples": total_raw_count,
        "balanced_train_samples": balanced_total,
        "metrics": {
            "roc_auc": round(roc_auc, 4),
            "pr_auc": round(pr_auc, 4),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4)
        },
        "model_export_path": model_save_path,
        "mlflow_registry_name": "FraudDetectionRandomForest",
        "duration_seconds": round(total_time, 2),
        "status": "SUCCESS",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    output_dir = "./data_output"
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "phase4_ml_training_summary.json")
    root_summary_path = "./phase4_summary.json"
    log_path = os.path.join(output_dir, "phase4_ml_training.log")
    
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_info, f, indent=2)
        
    with open(root_summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_info, f, indent=2)
        
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{summary_info['timestamp']}] Model: RandomForest | ROC-AUC: {round(roc_auc, 4)} | F1: {round(f1, 4)} | Status: SUCCESS\n")
        
    print(f" Saved Summary (Volume): {summary_path}")
    print(f" Saved Summary (Root):   {root_summary_path}")
    print(f" Saved Log:              {log_path}")
    
    spark.stop()
    return summary_info

def run_realtime_streaming_inference(
    model_path: str = "s3a://lakehouse-fraud/models/rf_fraud_model",
    kafka_bootstrap: str = None,
    kafka_input_topic: str = "creditcard-transactions",
    kafka_alert_topic: str = "fraud-alerts",
    checkpoint_path: str = "s3a://lakehouse-fraud/checkpoints/streaming_inference",
    minio_endpoint: str = None,
    stop_after_seconds: int = 0
):
    """
    Loads trained MLlib PipelineModel, connects to streaming Kafka topic, applies
    inference, and publishes detected fraud alerts (prediction == 1.0) to 'fraud-alerts' Kafka topic.
    """
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") is not None
    if not kafka_bootstrap:
        kafka_bootstrap = "kafka:9092" if is_docker else "localhost:9092"

    spark = create_ml_spark_session(minio_endpoint=minio_endpoint, app_name="FraudRealtimeInference")
    schema = define_transaction_schema()
    
    print("\n" + "="*80)
    print(" PHASE 4: REAL-TIME STREAMING INFERENCE & KAFKA ALERTING")
    print("="*80)
    print(f" Model Path:         {model_path}")
    print(f" Kafka Source Topic: {kafka_input_topic}")
    print(f" Kafka Alert Topic:  {kafka_alert_topic}")
    print(f" Checkpoint Path:    {checkpoint_path}")
    print("="*80 + "\n")
    
    # Load fitted PipelineModel
    print(f"[INFO] Loading PipelineModel from {model_path}...")
    try:
        model = PipelineModel.load(model_path)
        print("[SUCCESS] PipelineModel loaded successfully!")
    except Exception as e:
        print(f"[ERROR] Failed to load PipelineModel from {model_path}: {e}")
        spark.stop()
        return
        
    # Read Stream from Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_input_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        
    # Parse JSON Payload
    parsed_df = kafka_stream_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")
        
    # Apply ML Model Transformations
    print("[INFO] Transforming streaming micro-batches with ML Model...")
    predictions = model.transform(parsed_df)
    
    # Filter Detected Fraud Alerts (prediction == 1.0)
    fraud_alerts = predictions.filter(col("prediction") == 1.0) \
        .select(
            col("event_timestamp"),
            col("Time"),
            col("Amount"),
            col("prediction"),
            col("Class").alias("actual_label")
        )
        
    # Prepare JSON payload for Kafka Alert Topic
    alert_stream_json = fraud_alerts \
        .selectExpr("CAST(event_timestamp AS STRING) AS key", "to_json(struct(*)) AS value")
        
    print(f"[INFO] Publishing Fraud Alerts to Kafka topic '{kafka_alert_topic}'...")
    
    query = alert_stream_json.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", kafka_alert_topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="3 seconds") \
        .start()
        
    print(f"[SUCCESS] Streaming Real-Time Inference Active (Query ID: {query.id})")
    
    try:
        if stop_after_seconds > 0:
            print(f"[INFO] Running real-time inference for {stop_after_seconds} seconds...")
            time.sleep(stop_after_seconds)
            print("[INFO] Stopping inference query gracefully...")
            query.stop()
        else:
            print("Streaming inference active. Press Ctrl+C to stop...\n")
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Streaming inference stopped manually.")
        query.stop()
    finally:
        spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Phase 4 - Spark MLlib Fraud Model Training & Real-Time Inference")
    parser.add_argument("--mode", type=str, choices=["train", "infer", "all"], default="all", help="Execution mode: 'train', 'infer', or 'all'")
    parser.add_argument("--train_csv", type=str, default="./data_output/train_split.csv", help="Training CSV path")
    parser.add_argument("--val_csv", type=str, default="./data_output/test_split.csv", help="Validation CSV path")
    parser.add_argument("--model_path", type=str, default="s3a://lakehouse-fraud/models/rf_fraud_model", help="Model export path")
    parser.add_argument("--kafka", type=str, default=None, help="Kafka bootstrap servers")
    parser.add_argument("--input_topic", type=str, default="creditcard-transactions", help="Kafka input topic")
    parser.add_argument("--alert_topic", type=str, default="fraud-alerts", help="Kafka alert topic")
    parser.add_argument("--minio_endpoint", type=str, default=None, help="MinIO S3A endpoint")
    parser.add_argument("--trees", type=int, default=100, help="Random Forest numTrees")
    parser.add_argument("--depth", type=int, default=10, help="Random Forest maxDepth")
    parser.add_argument("--stop_after", type=int, default=30, help="Inference run duration in seconds")
    parser.add_argument("--report", action="store_true", help="Generate summary report")
    
    args = parser.parse_args()
    
    if args.mode in ["train", "all"]:
        train_and_register_fraud_model(
            train_csv_path=args.train_csv,
            val_csv_path=args.val_csv,
            model_save_path=args.model_path,
            minio_endpoint=args.minio_endpoint,
            num_trees=args.trees,
            max_depth=args.depth,
            use_undersampling=True
        )
        
    if args.mode in ["infer", "all"]:
        run_realtime_streaming_inference(
            model_path=args.model_path,
            kafka_bootstrap=args.kafka,
            kafka_input_topic=args.input_topic,
            kafka_alert_topic=args.alert_topic,
            minio_endpoint=args.minio_endpoint,
            stop_after_seconds=args.stop_after
        )

if __name__ == "__main__":
    main()

