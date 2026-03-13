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

### Producer
Le module producer.py est utilisé pour implémenter le comportement du producer Kafka. Dans ce module les données de l'API sont récupérées et envoyés au topic