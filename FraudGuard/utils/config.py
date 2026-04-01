import os

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TRANSACTIONS_TOPIC = 'transactions'
FRAUD_PREDICTIONS_TOPIC = 'fraud_predictions'

# Database Configuration
# For SQLite (simpler for local setup)
DATABASE_URL = 'sqlite:///fraud_detection.db'

# For PostgreSQL (if preferred)
# DATABASE_URL = 'postgresql://username:password@localhost:5432/fraud_detection'

# Model Configuration
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'ml_model', 'fraud_model.pkl')

# Spark Configuration
SPARK_APP_NAME = 'FraudDetectionStreaming'
SPARK_MASTER = 'local[*]'

# Transaction Generation
TRANSACTION_INTERVAL = 2  # seconds

# Feature Engineering
FEATURE_COLUMNS = [
    'transaction_amount',
    'merchant_category',
    'country',
    'transaction_frequency',
    'amount_avg',
    'amount_std',
    'country_change',
    'time_diff'
]