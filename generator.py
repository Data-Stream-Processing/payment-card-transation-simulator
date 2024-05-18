import json
import random

def generate_gps():
    # Generates random GPS coordinates (latitude, longitude)
    latitude = round(random.uniform(-90, 90), 6)
    longitude = round(random.uniform(-180, 180), 6)
    return (latitude, longitude)

def generate_transaction(card_id, user_id, spend_limit):
    gps_coords = generate_gps()
    transaction_value = round(random.uniform(1, spend_limit), 2)
    if random.random() < 0.05:  # 5% chance of anomaly
        transaction_value = round(transaction_value * 2, 2)  # Anomaly: sudden increase in transaction value
        gps_coords = generate_gps()  # Anomaly: sudden change in location
    return {
        "card_id": card_id,
        "user_id": user_id,
        "gps_coordinates": gps_coords,
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

def write_to_json(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)

# Simulate 10000 transactions for 10000 different cards
transactions = simulate_transactions(10000, 10000)
write_to_json("transactions.json", transactions)

print("Transactions generated and written to 'transactions.json'")
