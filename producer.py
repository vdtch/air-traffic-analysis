from kafka import KafkaProducer
import json
import requests
import time

from config.appconfig import KAFKA_BROKER, OPENSKY_API_URL, TOPIC

# Init Kafka producer
producer = KafkaProducer(
    bootstrap_servers= KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

# Read opensky API
def fetch_api_opensky():
    response = requests.get(OPENSKY_API_URL)
    if response.status_code == 200:
        return response.json().get("states", [])
    return []

# Send data to the kafka producer
while True:
    print("Fetching Open Sky data")
    states = fetch_api_opensky()
    for state in states:
        message = {
            "icao24": state[0],
            "callsign": state[1].strip(),
            "origin_country": state[2],
            "time_position": state[3],
            "last_contact": state[4],
            "longitude": state[5] if len(state) > 5 and state[5] is not None else 0,
            "latitude": state[6] if len(state) > 6 and state[6] is not None else 0,
            "baro_altitude": state[7],
            "on_ground": state[8] if len(state) > 8 and state[8] is not None else False,
            "velocity": state[9] if len(state) > 9 else 0,
            "true_track": state[10] if len(state) > 10 else 0,
            "vertical_rate": state[11] if len(state) > 11 else 0,
        }
        producer.send(TOPIC, message)
    
    print("Data sent to Kafka topic")
    time.sleep(10)  # Rafraîchissement toutes les 10 secondes

