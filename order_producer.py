import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = ["laptop", "mouse", "keyboard", "monitor", "headset"]
cities = ["Chicago", "New York", "Dallas", "San Jose", "Seattle"]

print("🚀 Order Producer Started...")

while True:
    order = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "amount": round(random.uniform(100, 2000), 2),
        "city": random.choice(cities),
    }
    print("➡️ Producing:", order)
    producer.send("orders", order)
    time.sleep(1)
