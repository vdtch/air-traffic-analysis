from kafka import KafkaConsumer
from config.appconfig import KAFKA_BROKER, TOPIC
from init_mongo import get_client, create_collection
import json

consumer = KafkaConsumer (
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer= lambda v: json.loads(v.decode("utf-8"))
)

# Connexion MongoDB
my_mongo_client = get_client()
my_flight_collection = create_collection(client=my_mongo_client, database_name="opensky", collection_name="flights")

# Traitement des données et stockage dans MongoDB
for message in consumer:
    flight_data = message.value
    print("Données air traffic")
    print(flight_data)
    print("Inserting into MongoDB:", flight_data)
    my_flight_collection.insert_one(flight_data)
    