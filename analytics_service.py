import json
from kafka import KafkaConsumer
from collections import defaultdict

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

sales = defaultdict(int)

print("📊 Analytics Service Started...")

for msg in consumer:
    order = msg.value
    sales[order["product"]] += 1
    print("📈 Live Sales Count:", dict(sales))

