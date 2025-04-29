from confluent_kafka import Consumer, Producer
from pymongo import MongoClient
from src.data_ingestion.validator import parse_transaction, validate_business_rules, transaction_to_serializable
import json


class TransactionConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'darooghe_consumer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})
        
    def process_message(self, msg):
        """Process a Kafka message with full schema validation."""
        transaction = parse_transaction(msg.value())
        if not transaction:
            return

        errors = validate_business_rules(transaction)
        if errors:
            self.producer.produce(
                'darooghe.error_logs',
                value=json.dumps({
                    'transaction_id': transaction.transaction_id,
                    'errors': errors,
                    'original_data':transaction_to_serializable(transaction)
                })
            )
            print(f"❌ Invalid: {transaction.transaction_id} ({errors})")
        else:
            self.producer.produce(
                'darooghe.valid_transactions',
                value=json.dumps(transaction_to_serializable(transaction))
            )
            print(f"✅ Valid: {transaction.transaction_id} - Stored in valid_transactions topic")

    def start_consuming(self):
        self.consumer.subscribe(['darooghe.transactions'])
        try:
            # Display one sample message
            msg = self.consumer.poll(10.0)
            if msg:
                transaction = parse_transaction(msg.value())
                if transaction:
                    print("\n=== SAMPLE MESSAGE ===")
                    print(json.dumps(transaction_to_serializable(transaction), indent=2))
                    print("=====================\n")
                    self.process_message(msg)

            # Process remaining messages
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                self.process_message(msg)
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()



if __name__ == "__main__":
    consumer = TransactionConsumer()
    consumer.start_consuming()