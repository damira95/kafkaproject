import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print("💳 Payments Service Started...")

for msg in consumer:
    order = msg.value
    print(f"[PAYMENTS] Charging customer ${order['amount']} for {order['product']} from {order['city']}")
