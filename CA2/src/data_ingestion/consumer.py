from confluent_kafka import Consumer, Producer
import json
from datetime import datetime, timedelta

class TransactionConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'darooghe_consumer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def validate_transaction(self, transaction):
        """Check the 3 validation rules"""
        errors = []
        
        # Rule 1: Amount Consistency
        calculated = transaction['amount'] + transaction['vat_amount'] + transaction['commission_amount']
        if abs(calculated - transaction['total_amount']) > 0.01:
            errors.append("ERR_AMOUNT")
            
        # Rule 2: Time Warping
        tx_time = datetime.fromisoformat(transaction['timestamp'].replace('Z', ''))
        now = datetime.utcnow()
        if tx_time > now or (now - tx_time) > timedelta(days=1):
            errors.append("ERR_TIME")
            
        # Rule 3: Device Mismatch
        if (transaction['payment_method'] == "mobile" and 
            'device_info' in transaction and  # Add this check
            transaction['device_info'].get('os') not in ['iOS', 'Android']):
            errors.append("ERR_DEVICE")
            
        return errors

    def process_message(self, msg):
        try:
            transaction = json.loads(msg.value())
            errors = self.validate_transaction(transaction)
            
            if errors:
                error_msg = {
                    'transaction_id': transaction['transaction_id'],
                    'errors': errors,
                    'original_data': transaction
                }
                self.producer.produce(
                    'darooghe.error_logs',
                    value=json.dumps(error_msg)
                )
                print(f"❌ Invalid transaction: {transaction['transaction_id']}")
            else:
                print(f"✅ Valid transaction: {transaction['transaction_id']}")
                
        except json.JSONDecodeError:
            print("Failed to decode message")

    def start_consuming(self):
        self.consumer.subscribe(['darooghe.transactions'])

        # III) Display 1 message
        print("Waiting for first message...")
        first_msg = None
        while not first_msg:
            first_msg = self.consumer.poll(1.0)
            if first_msg is None:
                print("Waiting for messages...")
                continue
            if first_msg.error():
                print(f"Initial message error: {first_msg.error()}")
                continue
        
        print("\n=== SAMPLE MESSAGE ===")
        print(json.dumps(json.loads(first_msg.value()), indent=2))
        print("=====================\n")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue
                
                self.process_message(msg)
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = TransactionConsumer()
    print("Starting consumer...")
    consumer.start_consuming()