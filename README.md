# air-traffic-analysis
Système de monitoring et d’analyse du trafic aérien en temps réel basé sur les données de l’API OpenSky Network.

Le but de ce mini projet est de réaliser différents Use Case à partir des données qui seront captées via l'API.
Pour ce faire, l'architecture pensée s'articule comme suit :
- *kafka* pour effectuer la *lecture en continu* des données de l'API
- *mongodb* sera utilisée pour stocker les données traitées
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
Il fonctionne avec des topics où les producteurs publient des messages et les consommateurs les lisent de manière asynchrone. Cette dernière signifie que une fois les messages publiés par les proudcteurs, les consommateurs les consomment à leur rythme. Kafka ne force par la lecture immédiate des messages.