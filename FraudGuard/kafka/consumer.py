import json
from kafka import KafkaConsumer
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import KAFKA_BROKER, FRAUD_PREDICTIONS_TOPIC

class FraudConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            FRAUD_PREDICTIONS_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume_predictions(self):
        """
        Consume fraud predictions from Kafka
        """
        print("Starting fraud predictions consumer...")

        try:
            for message in self.consumer:
                prediction = message.value
                print(f"Received prediction: {prediction}")

                # Here you could process the predictions further
                # e.g., send to dashboard, store in additional systems, etc.

        except KeyboardInterrupt:
            print("Stopping consumer...")
            self.consumer.close()

if __name__ == "__main__":
    consumer = FraudConsumer()
    consumer.consume_predictions()