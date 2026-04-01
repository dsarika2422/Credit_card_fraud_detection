import json
import sys
import os
import time
import threading
import queue
from kafka import KafkaConsumer, KafkaProducer
import joblib
import pandas as pd
import numpy as np
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    KAFKA_BROKER, TRANSACTIONS_TOPIC, FRAUD_PREDICTIONS_TOPIC,
    MODEL_PATH
)
from utils.feature_engineering import FeatureEngineer
from database.db_setup import get_session, Transaction, FraudPrediction

class SimplifiedStreamingProcessor:
    def __init__(self):
        self.model_data = None
        self.feature_engineer = FeatureEngineer()
        self.consumer = None
        self.producer = None
        self.running = False
        self.transaction_queue = queue.Queue()

    def load_model(self):
        """
        Load trained ML model
        """
        try:
            self.model_data = joblib.load(MODEL_PATH)
            print("Model loaded successfully")
        except Exception as e:
            print(f"Error loading model: {e}")
            raise

    def create_kafka_connections(self):
        """
        Create Kafka consumer and producer
        """
        try:
            self.consumer = KafkaConsumer(
                TRANSACTIONS_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            print("Kafka connections established")

        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            print("Note: Make sure Kafka is running locally on localhost:9092")
            raise

    def process_transaction(self, transaction):
        """
        Process a single transaction
        """
        try:
            # Convert to DataFrame for processing
            df = pd.DataFrame([transaction])

            # Basic feature engineering (simplified)
            features = self.extract_features(df)

            # Make prediction
            prediction = self.predict_fraud(features)

            # Store in database
            self.store_in_database(transaction, prediction)

            # Send to Kafka
            self.send_to_kafka(transaction, prediction)

            print(f"Processed transaction {transaction['transaction_id']}: "
                  f"Fraud probability = {prediction['fraud_probability']:.3f}")

        except Exception as e:
            print(f"Error processing transaction: {e}")

    def extract_features(self, df):
        """
        Extract features from transaction (simplified version)
        """
        features = {
            'transaction_amount': df['transaction_amount'].iloc[0],
            'merchant_category': df['merchant_category'].iloc[0],
            'country': df['country'].iloc[0],
            'transaction_frequency': 1.0,  # Simplified
            'amount_avg': df['transaction_amount'].iloc[0],  # Simplified
            'amount_std': 0.0,  # Simplified
            'country_change': 0,  # Simplified
            'time_diff': 3600.0  # Simplified
        }

        return features

    def predict_fraud(self, features):
        """
        Predict fraud probability
        """
        if self.model_data is None:
            return {
                'fraud_probability': 0.5,
                'fraud_label': False
            }

        # Prepare features for model
        feature_df = pd.DataFrame([features])

        # Encode categorical features
        feature_df = pd.get_dummies(feature_df, columns=['merchant_category', 'country'])

        # Ensure all required columns are present
        model = self.model_data['model']
        scaler = self.model_data['scaler']
        feature_columns = self.model_data['feature_columns']

        # Add missing columns with 0
        for col in feature_columns:
            if col not in feature_df.columns:
                feature_df[col] = 0

        # Keep only required columns
        X = feature_df[feature_columns]

        # Scale features
        X_scaled = scaler.transform(X)

        # Predict
        fraud_proba = model.predict_proba(X_scaled)[:, 1]
        fraud_label = model.predict(X_scaled)

        return {
            'fraud_probability': float(fraud_proba[0]),
            'fraud_label': bool(fraud_label[0])
        }

    def store_in_database(self, transaction, prediction):
        """
        Store transaction and prediction in database
        """
        try:
            session = get_session()

            # Store transaction
            trans = Transaction(
                transaction_id=transaction['transaction_id'],
                card_id=transaction['card_id'],
                amount=transaction['transaction_amount'],
                merchant=transaction['merchant_name'],
                country=transaction['country'],
                timestamp=pd.to_datetime(transaction['timestamp']),
                transaction_type=transaction['transaction_type']
            )
            session.merge(trans)

            # Store prediction
            pred = FraudPrediction(
                transaction_id=transaction['transaction_id'],
                risk_score=prediction['fraud_probability'],
                fraud_label=prediction['fraud_label']
            )
            session.merge(pred)

            session.commit()
            session.close()

        except Exception as e:
            print(f"Error storing in database: {e}")

    def send_to_kafka(self, transaction, prediction):
        """
        Send prediction to Kafka
        """
        try:
            message = {
                'transaction_id': transaction['transaction_id'],
                'fraud_probability': prediction['fraud_probability'],
                'fraud_label': prediction['fraud_label'],
                'timestamp': datetime.now().isoformat()
            }

            self.producer.send(FRAUD_PREDICTIONS_TOPIC, message)

        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    def consumer_thread(self):
        """
        Thread to consume transactions from Kafka
        """
        print("Starting transaction consumer...")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                transaction = message.value
                self.transaction_queue.put(transaction)

        except Exception as e:
            print(f"Consumer error: {e}")

    def processor_thread(self):
        """
        Thread to process transactions
        """
        print("Starting transaction processor...")

        while self.running:
            try:
                # Get transaction from queue (with timeout)
                transaction = self.transaction_queue.get(timeout=1)
                self.process_transaction(transaction)
                self.transaction_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Processor error: {e}")

    def run(self):
        """
        Main method to run the processor
        """
        try:
            print("Starting Simplified Streaming Processor...")
            print("Note: This version works without Apache Spark for local demo")

            self.load_model()
            self.create_kafka_connections()

            self.running = True

            # Start consumer and processor threads
            consumer_thread = threading.Thread(target=self.consumer_thread)
            processor_thread = threading.Thread(target=self.processor_thread)

            consumer_thread.start()
            processor_thread.start()

            print("Streaming processor is running. Press Ctrl+C to stop.")

            # Keep main thread alive
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Stopping processor...")

            self.running = False

            # Wait for threads to finish
            consumer_thread.join(timeout=5)
            processor_thread.join(timeout=5)

            # Close connections
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

            print("Processor stopped.")

        except Exception as e:
            print(f"Error in streaming processor: {e}")

if __name__ == "__main__":
    processor = SimplifiedStreamingProcessor()
    processor.run()