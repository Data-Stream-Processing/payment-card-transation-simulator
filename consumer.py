from kafka import KafkaConsumer
import json

# Kafka Consumer
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == '__main__':
    print("Starting consumer...")
    for message in consumer:
        transaction = message.value
        print(f"Consumed message: {transaction}")