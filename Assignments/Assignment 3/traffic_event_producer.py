# producer.py
import time
import random
import json
from kafka import KafkaProducer

# pip install kafka-python

TOPIC = "traffic_events"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = ['S1','S2','S3','S4','S5']
congestion_level = ["LOW", "HIGH", "MEDIUM"]

while True:
    event = {
        "sensor_id": random.choice(sensor_id),
        "timestamp": time.time(),
	"vehicle_count": random.randint(0,100),
	"avg_speed": random.uniform(10,140),
        "congestion_level": random.choice(congestion_level)
    }
    producer.send(TOPIC, event)
    print(f"Sent event: {event}")
    time.sleep(random.uniform(0.5, 2.0))  # random interval
