import sys
import os
import time
import threading
import queue
import json
from datetime import datetime
import pandas as pd
import numpy as np
from faker import Faker

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import MODEL_PATH
from utils.feature_engineering import FeatureEngineer
from database.db_setup import get_session, Transaction, FraudPrediction
import joblib

fake = Faker()

class FraudDetectionDemo:
    def __init__(self):
        self.model_data = None
        self.feature_engineer = FeatureEngineer()
        self.transaction_queue = queue.Queue()
        self.prediction_queue = queue.Queue()
        self.running = False

    def load_model(self):
        """Load the trained ML model"""
        try:
            self.model_data = joblib.load(MODEL_PATH)
            print("✅ Model loaded successfully")
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            return False
        return True

    def generate_transaction(self):
        """Generate a fake transaction"""
        # Use some cards more frequently
        card_ids = [
            '1234567890123456',
            '9876543210987654',
            '5555666677778888',
            '1111222233334444'
        ]

        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'card_id': np.random.choice(card_ids),
            'transaction_amount': round(np.random.exponential(100), 2),
            'merchant_name': fake.company(),
            'merchant_category': np.random.choice([
                'Retail', 'Online Retail', 'Food', 'Travel',
                'Electronics', 'Gas Station', 'Hotel', 'Finance'
            ]),
            'country': np.random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'RU', 'CH'],
                                       p=[0.6, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05]),
            'timestamp': datetime.now().isoformat(),
            'transaction_type': 'purchase'
        }

        return transaction

    def predict_fraud(self, features):
        """Predict fraud probability"""
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

    def process_transaction(self, transaction):
        """Process a single transaction"""
        try:
            # Convert to DataFrame for processing
            df = pd.DataFrame([transaction])

            # Basic feature engineering
            features = {
                'transaction_amount': df['transaction_amount'].iloc[0],
                'merchant_category': df['merchant_category'].iloc[0],
                'country': df['country'].iloc[0],
                'transaction_frequency': 1.0,
                'amount_avg': df['transaction_amount'].iloc[0],
                'amount_std': 0.0,
                'country_change': 0,
                'time_diff': 3600.0
            }

            # Make prediction
            prediction = self.predict_fraud(features)

            # Store in database
            self.store_in_database(transaction, prediction)

            # Put prediction in queue for dashboard
            self.prediction_queue.put({
                'transaction': transaction,
                'prediction': prediction
            })

            print(f"🔍 Processed transaction {transaction['transaction_id']}: "
                  f"Amount=${transaction['transaction_amount']:.2f}, "
                  f"Risk={prediction['fraud_probability']:.3f}, "
                  f"Fraud={'🚨 YES' if prediction['fraud_label'] else '✅ NO'}")

        except Exception as e:
            print(f"❌ Error processing transaction: {e}")

    def store_in_database(self, transaction, prediction):
        """Store transaction and prediction in database"""
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
            print(f"❌ Error storing in database: {e}")

    def transaction_generator_thread(self):
        """Thread to generate transactions"""
        print("🏦 Starting transaction generator...")

        while self.running:
            transaction = self.generate_transaction()
            self.transaction_queue.put(transaction)
            time.sleep(2)  # Generate every 2 seconds

    def transaction_processor_thread(self):
        """Thread to process transactions"""
        print("⚡ Starting transaction processor...")

        while self.running:
            try:
                transaction = self.transaction_queue.get(timeout=1)
                self.process_transaction(transaction)
                self.transaction_queue.task_done()
            except queue.Empty:
                continue

    def run_demo(self):
        """Run the complete fraud detection demo"""
        print("🚀 Starting Real-Time Fraud Detection Demo")
        print("=" * 50)

        # Load model
        if not self.load_model():
            return

        # Setup database
        try:
            from database.db_setup import setup_database
            setup_database()
            print("✅ Database ready")
        except Exception as e:
            print(f"❌ Database setup error: {e}")
            return

        self.running = True

        # Start threads
        generator_thread = threading.Thread(target=self.transaction_generator_thread)
        processor_thread = threading.Thread(target=self.transaction_processor_thread)

        generator_thread.start()
        processor_thread.start()

        print("\n🎯 Demo is running!")
        print("📊 Transactions are being generated and processed every 2 seconds")
        print("💾 Data is stored in SQLite database")
        print("🔍 Fraud detection using machine learning")
        print("\nPress Ctrl+C to stop the demo\n")

        try:
            # Show live stats every 10 seconds
            start_time = time.time()
            transaction_count = 0

            # Run for only 30 seconds for demo
            end_time = time.time() + 30

            while self.running and time.time() < end_time:
                time.sleep(2)  # Check every 2 seconds
                transaction_count += 1  # Approximate

                # Get current stats
                session = get_session()
                total_txns = session.query(Transaction).count()
                fraud_txns = session.query(FraudPrediction).filter(FraudPrediction.fraud_label == True).count()
                session.close()

                fraud_rate = (fraud_txns / total_txns * 100) if total_txns > 0 else 0

                print(f"📈 Progress: {total_txns} transactions processed, "
                      f"{fraud_txns} fraudulent ({fraud_rate:.1f}%)")

            print("\n🎉 Demo completed successfully!")

        except KeyboardInterrupt:
            print("\n🛑 Stopping demo...")

        self.running = False

        # Wait for threads
        generator_thread.join(timeout=5)
        processor_thread.join(timeout=5)

        print("✅ Demo completed!")
        print("\n💡 Next steps:")
        print("1. Run the dashboard: streamlit run dashboard/dashboard.py")
        print("2. Or check the API: uvicorn api.app:app --reload")
        print("3. View stored data in fraud_detection.db")

if __name__ == "__main__":
    import uuid  # Import here to avoid import issues

    demo = FraudDetectionDemo()
    demo.run_demo()