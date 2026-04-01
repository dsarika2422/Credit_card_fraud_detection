# Real-Time Credit Card Fraud Detection System

A complete end-to-end fraud detection pipeline demonstrating big data streaming, machine learning, and real-time dashboards.

## Architecture

Transaction Generator → Kafka → Spark Streaming → Feature Engineering → ML Model → Database → Dashboard

## Technologies

- **Programming**: Python
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark (PySpark)
- **ML**: Scikit-learn, XGBoost
- **Database**: PostgreSQL/SQLite
- **Dashboard**: Streamlit
- **API**: FastAPI

## Setup Instructions

### Prerequisites

1. **Python 3.8+**
2. **Apache Kafka** (local setup)
3. **Apache Spark** (local setup)
4. **PostgreSQL** or use SQLite

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Kafka Setup

1. Download and extract Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
2. Start Zookeeper:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
3. Start Kafka server:
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```
4. Create topics:
   ```bash
   bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic fraud_predictions --bootstrap-server localhost:9092
   ```

### Database Setup

Run the database setup script:

```bash
python database/db_setup.py
```

### Train ML Model

```bash
python ml_model/train_model.py
```

### Run Components

1. **Start Spark Streaming Processor**:
   ```bash
   python spark/spark_stream_processor.py
   ```

2. **Start Transaction Producer**:
   ```bash
   python kafka/producer.py
   ```

3. **Start Dashboard**:
   ```bash
   streamlit run dashboard/dashboard.py
   ```

4. **Optional: Start API**:
   ```bash
   uvicorn api.app:app --reload
   ```

## Project Structure

```
fraud-detection-system/
├── data/
│   └── sample_transactions.csv  # 100 diverse transactions with fraud indicators
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── spark/
│   └── spark_stream_processor.py
├── ml_model/
│   ├── train_model.py
│   └── fraud_model.pkl
├── database/
│   └── db_setup.py
├── api/
│   └── app.py
├── dashboard/
│   └── dashboard.py
├── utils/
│   ├── feature_engineering.py
│   └── config.py
├── requirements.txt
└── README.md
```

## Usage

Once all components are running:

1. Transactions are generated every 1-2 seconds
2. Spark processes them in real-time
3. ML model predicts fraud probability
4. Results are stored in database
5. Dashboard updates live with analytics

## Features

- Real-time transaction streaming
- Feature engineering for fraud detection
- Machine learning fraud prediction
- Interactive dashboard with charts and metrics
- Database storage for historical data