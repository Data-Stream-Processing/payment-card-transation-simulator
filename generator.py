import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

def generate_gps():
    # Generates random GPS coordinates (latitude, longitude)
    latitude = round(random.uniform(-90, 90), 6)
    longitude = round(random.uniform(-180, 180), 6)
    return latitude, longitude

def generate_transaction(card_id, user_id, spend_limit):
    latitude, longitude = generate_gps()
    transaction_value = round(random.uniform(1, spend_limit), 2)
    if random.random() < 0.05:  # 5% chance of anomaly
        transaction_value = round(transaction_value * 2, 2)  # Anomaly: sudden increase in transaction value
        latitude, longitude = generate_gps()  # Anomaly: sudden change in location
    return {
        "card_id": card_id,
        "user_id": user_id,
        "latitude": latitude,
        "longitude": longitude,
        "transaction_value": transaction_value,
        "spend_limit": spend_limit
    }

def simulate_transactions(num_cards, num_transactions):
    transactions = []
    user_ids = {i: random.randint(1, 3000) for i in range(num_cards)}  # Each card maps to one of 3000 users
    spend_limits = {i: round(random.uniform(100, 5000), 2) for i in range(num_cards)}  # Random spend limit for each card rounded to 2 decimal places

    for _ in range(num_transactions):
        card_id = random.choice(list(user_ids.keys()))
        user_id = user_ids[card_id]
        spend_limit = spend_limits[card_id]
        transaction = generate_transaction(card_id, user_id, spend_limit)
        transactions.append(transaction)

    return transactions

def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':
    num_cards = 10000
    num_transactions = 10000
    transactions = simulate_transactions(num_cards, num_transactions)

    # Infinite loop - runs until you kill the program
    for transaction in transactions:
        # Send it to our 'transactions' topic
        print(f'Producing transaction @ {datetime.now()} | Transaction = {transaction}')
        producer.send('transactions', transaction)
        # Sleep for a random number of seconds
        time_to_sleep = random.randint(1, 11)
        time.sleep(time_to_sleep)

    write_to_json("transactions.json", transactions)
    print("Transactions generated and sent to Kafka topic 'transactions'")
