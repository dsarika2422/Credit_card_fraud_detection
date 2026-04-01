import json
import time
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import KAFKA_BROKER, TRANSACTIONS_TOPIC, TRANSACTION_INTERVAL

fake = Faker()

class TransactionGenerator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Track card histories for realistic generation
        self.card_histories = {}

    def generate_transaction(self):
        """
        Generate a fake credit card transaction
        """
        # Use some cards more frequently
        card_weights = [0.3, 0.2, 0.15, 0.15, 0.1, 0.1]
        card_ids = [
            '1234567890123456',
            '9876543210987654',
            '5555666677778888',
            '1111222233334444',
            '9999000011112222',
            '7777888899990000'
        ]

        card_id = np.random.choice(card_ids, p=card_weights)

        # Initialize card history if new
        if card_id not in self.card_histories:
            self.card_histories[card_id] = {
                'last_country': 'US',
                'last_timestamp': datetime.now(),
                'transaction_count': 0
            }

        history = self.card_histories[card_id]

        # Generate transaction
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'card_id': card_id,
            'transaction_amount': round(np.random.exponential(100), 2),
            'merchant_name': fake.company(),
            'merchant_category': np.random.choice([
                'Retail', 'Online Retail', 'Food', 'Travel',
                'Electronics', 'Gas Station', 'Hotel', 'Finance'
            ]),
            'country': np.random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'RU', 'CH', 'JP'],
                                       p=[0.6, 0.1, 0.1, 0.05, 0.05, 0.05, 0.03, 0.02]),
            'timestamp': datetime.now().isoformat(),
            'transaction_type': 'purchase'
        }

        # Update history
        history['last_country'] = transaction['country']
        history['last_timestamp'] = datetime.now()
        history['transaction_count'] += 1

        return transaction

    def send_transaction(self, transaction):
        """
        Send transaction to Kafka
        """
        try:
            future = self.producer.send(TRANSACTIONS_TOPIC, transaction)
            record_metadata = future.get(timeout=10)

            print(f"Sent transaction {transaction['transaction_id']} to topic {record_metadata.topic} "
                  f"partition {record_metadata.partition} offset {record_metadata.offset}")

        except Exception as e:
            print(f"Error sending transaction: {e}")

    def run(self):
        """
        Main loop to generate and send transactions
        """
        print("Starting transaction generator...")

        try:
            while True:
                transaction = self.generate_transaction()
                self.send_transaction(transaction)

                time.sleep(TRANSACTION_INTERVAL)

        except KeyboardInterrupt:
            print("Stopping transaction generator...")
            self.producer.close()

if __name__ == "__main__":
    import numpy as np

    generator = TransactionGenerator()
    generator.run()