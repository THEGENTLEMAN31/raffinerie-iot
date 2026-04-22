# 🏗️ Raffinerie-IoT : Pipeline Big Data Industrielle

Ce projet simule une architecture de surveillance en temps réel pour une raffinerie industrielle. Il met en œuvre une pipeline complète allant de la capture de données IoT à la visualisation dynamique.

## 🚀 Architecture du Système

La pipeline est composée des briques technologiques suivantes :
*   **Simulateur IoT (Python) :** Génère des données de température et de vibration avec une logique physique réaliste (inertie, dérive, pannes).
*   **Mosquitto (MQTT) :** Broker de messages pour la collecte des données capteurs.
*   **Kafka :** Backbone de streaming pour l'ingestion massive des données.
*   **Spark Structured Streaming :** Moteur de traitement temps réel pour le filtrage et le calcul de KPI (moyennes glissantes).
*   **TimescaleDB :** Base de données relationnelle optimisée pour les séries temporelles.
*   **MinIO (S3) :** Stockage d'objets pour l'archivage des données brutes (Data Lake).
*   **Grafana :** Dashboarding dynamique pour le monitoring en temps réel.

## 🛠️ Installation et Lancement

### 1. Prérequis
*   Docker & Docker Compose
*   Python 3.14+
*   Git

### 2. Démarrage de l'Infrastructure
```bash
cd raffinerie-iot
docker-compose up -d
```

### 3. Lancement des composants Python
Activez votre environnement virtuel et lancez les scripts :
```bash
# Dans un terminal
source venv/bin/activate
python simulateur_capteurs.py

# Dans un autre terminal
source venv/bin/activate
python mqtt_to_kafka.py
```

### 4. Lancement du Traitement Spark
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
  /app/traitement_kpi.py
```

## 📊 Visualisation
Accédez à Grafana sur [http://localhost:3000](http://localhost:3000) (admin/admin) et connectez la source PostgreSQL `timescaledb:5432`.

## 🧠 Logique de Simulation
Le simulateur utilise un modèle de **lissage exponentiel** pour simuler l'inertie thermique et bascule aléatoirement entre trois scénarios :
1.  **NORMAL** (Fonctionnement nominal)
2.  **SURCHAUFFE** (Dérive lente de la température)
3.  **USURE MÉCANIQUE** (Augmentation des vibrations)

---
*Projet réalisé dans le cadre du parcours IA & Big Data.*
