from kafka import KafkaProducer
import json

from config import KAFKA_BROKER, TOPIC

# Init Kafka producer
producer = KafkaProducer(
    bootstrap_servers= KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

