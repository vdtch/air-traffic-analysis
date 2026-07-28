# air-traffic-analysis
Système de monitoring et d’analyse du trafic aérien en temps réel basé sur les données de l’API OpenSky Network.

Le but de ce mini projet est de suivre en temps réel la position des vols à partir des données qui seront captées via l'API.
Pour ce faire, l'architecture pensée s'articule comme suit :
- *kafka* pour effectuer la *lecture en continu* des données de l'API
- *mongodb* sera utilisée pour stocker les données lues
- *les traitements* seront effectués en *pyspark* avant d'être *persistés* dans Mongo
Plus tard du *Prefect* sera rajouté pour *l'orchestration*

## Création Environnement virtuel
Placez-vous dans votre terminal ou ouvrez votre CMD si vous êtes sous windows.
```
python -m venv venv
```
où venv est le dossier dans lequel seront placées vos librairies python

### Windows venv activation
Pour activer votre venv sur Windows, il vous faut exécuter le script installé par venv :
```
venv\Scripts\activate.bat
```

Pour désactiver le venv, juste rentrer *deactivate*


## Lecture des données de l'API
Comme indiqué plus haut, la lecture des données sera faite via *Kafka*. 
Il s'agit d'une plateforme de streaming distribuée qui permet de collecter, traiter et diffuser des flux de données en temps réel entre différents systèmes de manière fiable et scalable. 
Il fonctionne avec des topics où les producteurs publient des messages et les consommateurs les lisent de manière asynchrone. Cette dernière signifie qu'une fois les messages publiés par les proudcteurs, les consommateurs les consomment à leur rythme. Kafka ne force par la lecture immédiate des messages.

L'API à interroger est celle-ci : https://opensky-network.org/api/states/all
La liste des données utiles à notre Use Case :

| Index | Property | Type | Description
| ----- |----------|------|------------
|0  |icao24   |string |  Unique ICAO 24-bit address of the transponder in hex string representation.
|1  |callsign |string | Callsign of the vehicle (8 chars). Can be null if no callsign has been received.
|2  |origin_country |string | Country name inferred from the ICAO 24-bit address.
|3  |time_position |int | Unix timestamp (seconds) for the last position update. Can be null if no position report was received by OpenSky within the past 15s.
|4  |last_contact |int | Unix timestamp (seconds) for the last update in general.
|5  |longitude |float | WGS-84 longitude in decimal degrees. Can be null.
|6  |latitude |float | WGS-84 latitude in decimal degrees. Can be null.
|7  |baro_altitude |float | Barometric altitude in meters. Can be null.
|8  |on_ground |boolean | Boolean value which indicates if the position was retrieved from a surface position report.
|9  |velocity |float | Velocity over ground in m/s. Can be null.
|10 |true_track |float | True track in decimal degrees clockwise from north (north=0°). Can be null
|11 |vertical_rate |float | Vertical rate in m/s. A positive value indicates that the airplane is climbing, a negative value indicates that it descends. Can be null.

## Traitement et persistance des données
Pour effectuer les traitements, j'ai organisé le code en différents modules : producer.py, consumer.py, init, config.
Afin de collecter et traiter les données, il faut mettre en place un cluster Kafka qui est constitué de serveur(s) appelés broker(s). L'ajout de broker dans un cluster Kafka permet tout simplement de monter en charge en cas de volumétrie de données importante.
Les données envoyés dans Kafka ou messages, sont organisés sous forme de topics - il s'agit de l'équivalent de tables en bases de données. Les topics sont à leur tout constitués de partitions servant à paralléliser la consommation de messages.

### Cluster Kafka
Pour configuer mon cluster en local, je suis passé par une image docker. Pour me simplifier la tâche, j'ai installé le client Docker - Docker Desktop. Pour récupérer et lancer mon image, j'utiliser un fichier docker-compose.yml : 
```
version: '3'

services:
  zookeeper:
    image : wurstmeister/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
  
  kafka:
    image : wurstmeister/kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

Placez ce fichier à la racine du projet, puis lancez le cluster :
```
docker compose up -d
```
Pour arrêter le cluster : `docker compose down`

### Producer
Le module producer.py implémente le comportement du producer Kafka. Les données de l'API y sont récupérées puis envoyées au topic *opensky-data*.

Le déroulement est le suivant :
1. Initialisation du producer avec l'adresse du broker et un sérialiseur JSON (`value_serializer`), car Kafka ne transporte que des octets.
2. Appel de l'API OpenSky via `requests`. La réponse contient une clé *states*, qui est une liste de listes : chaque sous-liste décrit un aéronef, position par position, selon le tableau ci-dessus.
3. Transformation de chaque liste en dictionnaire lisible (icao24, callsign, latitude, longitude, etc.) et envoi au topic.
4. Mise en veille de 10 secondes avant le prochain appel, pour ne pas saturer l'API.

Lancement :
```
python producer.py
```

### Consumer
Le module consumer.py lit les messages du topic, les stocke dans MongoDB et produit une visualisation.

Le déroulement est le suivant :
1. Initialisation du consumer sur le topic *opensky-data* avec un désérialiseur JSON.
2. Connexion à MongoDB et récupération de la collection cible via le module init_mongo.
3. Boucle infinie sur les messages : chaque message est inséré dans la collection *flights*.
4. Toutes les 10 itérations, les données accumulées sont chargées dans un DataFrame pandas, filtrées sur les vols en l'air (`on_ground == False`), puis affichées sur un nuage de points longitude/latitude coloré par la vitesse.

Lancement, dans un second terminal :
```
python consumer.py
```

### Persistance dans MongoDB
Le stockage est assuré par une base MongoDB Atlas. Le module init_mongo expose deux fonctions :
- `get_client()` : construit l'URI de connexion à partir de la configuration, ouvre le client et vérifie la connexion avec un *ping*.
- `create_collection()` : renvoie la collection demandée et la crée si elle n'existe pas encore.

Les données sont écrites dans la base *opensky*, collection *flights*.

Les identifiants ne sont jamais versionnés : ils sont lus depuis un fichier .env à la racine du projet, ignoré par git. Créez ce fichier sur le modèle suivant :
```
MONGO_HOSTNAME=<votre-cluster>.mongodb.net
MONGO_CLUSTERNAME=<nom-du-cluster>
MONGO_USERNAME=<utilisateur>
MONGO_PASSWORD=<mot-de-passe>
```

## Installation des dépendances
Une fois le venv activé :
```
pip install -r requirements.txt
```

## Structure du projet
```
air-traffic-analysis/
├── config/
│   ├── appconfig.py      # Paramètres Kafka et URL de l'API OpenSky
│   └── mongoconfig.py    # Lecture des variables .env et construction de l'URI Mongo
├── init_mongo.py         # Connexion MongoDB et création de la collection
├── producer.py           # Lecture de l'API et publication dans Kafka
├── consumer.py           # Consommation, stockage Mongo et visualisation
├── requirements.txt
└── README.md
```

## Ordre de lancement
1. Démarrer le cluster Kafka : `docker compose up -d`
2. Vérifier que le fichier .env est renseigné
3. Lancer le producer : `python producer.py`
4. Lancer le consumer dans un autre terminal : `python consumer.py`

## Prochaines étapes
- Remplacer les traitements pandas par du *PySpark* pour absorber une volumétrie plus importante
- Ajouter *Prefect* pour l'orchestration des flux
- Mettre en place des index MongoDB sur icao24 et time_position pour accélérer les requêtes
- Ajouter des tests et une gestion des erreurs sur les appels API