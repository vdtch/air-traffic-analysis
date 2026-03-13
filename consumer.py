from kafka import KafkaConsumer
from config.appconfig import KAFKA_BROKER, TOPIC
from init_mongo import get_client, create_collection

import json
import pandas as pd
import matplotlib.pyplot as plt


consumer = KafkaConsumer (
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer= lambda v: json.loads(v.decode("utf-8"))
)

# Connexion MongoDB
my_mongo_client = get_client()
my_flight_collection = create_collection(client=my_mongo_client, database_name="opensky", collection_name="flights")

# Traitement des données et stockage dans MongoDB
flight_data_list = []
for message in consumer:
    flight_data = message.value
    print("Données air traffic")
    print(flight_data)
    print("Inserting into MongoDB:", flight_data)
    my_flight_collection.insert_one(flight_data)
    flight_data_list.append(flight_data)

    # Analyse et visualisation toutes les 10 itérations
    if len(flight_data_list) % 10 == 0:
        df = pd.DataFrame(flight_data_list)
        
        # Filtrage des vols en vol
        if "on_ground" in df.columns:
            df = df[df["on_ground"] == False]
        
        df.columns
        
        # Création du graphique
        plt.figure(figsize=(10, 6))
        plt.scatter(df["longitude"], df["latitude"], c=df["velocity"], cmap='coolwarm', alpha=0.6)
        plt.colorbar(label='Vitesse (m/s)')
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.title("Carte des avions en vol avec vitesse")
        plt.show()
        
        # Réinitialisation des données
        flight_data_list = []
    