# Apache Superset Custom Configuration
import os

SECRET_KEY = "realtime_fraud_detection_lakehouse_secret_key"

# Enable SQLite database connections in Superset UI
PREVENT_UNSAFE_DB_CONNECTIONS = False
ALLOWED_EXTRA_CONNECTIONS = ["sqlite"]

# Enable SQL Lab & Template processing
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "SQLLAB_BACKEND_PERSISTENCE": False,
}
