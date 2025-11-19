import json
from kafka import KafkaConsumer

HIGH_RISK_CITIES = {"Seattle", "Dallas"}
AMOUNT_THRESHOLD = 1500

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("🕵️ Fraud Service Started...")

for msg in consumer:
    order = msg.value

    if order["amount"] > AMOUNT_THRESHOLD or order["city"] in HIGH_RISK_CITIES:
        print(f"🚨 FRAUD ALERT: {order}")
    else:
        print(f"[FRAUD] OK → {order}")
