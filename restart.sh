#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[1/6] Démarrage des conteneurs Docker..."
docker compose up -d

echo "[2/6] Attente de Kafka..."
for i in $(seq 30); do
  docker exec kafka bash -c "kafka-topics --bootstrap-server localhost:29092 --list" &>/dev/null && break
  sleep 2
done

echo "[3/6] Attente de Spark master..."
for i in $(seq 15); do
  docker exec spark-master bash -c "curl -s http://localhost:8080" &>/dev/null && break
  sleep 2
done

echo "[4/6] Vérification du modèle..."
docker exec spark-master bash -c "
  if [ ! -d /home/spark/.ivy2/predictive_model ]; then
    cp -r /app/predictive_model /home/spark/.ivy2/predictive_model
    echo 'Modèle copié vers /home/spark/.ivy2/predictive_model'
  else
    echo 'Modèle déjà présent'
  fi
"

echo "[5/6] Checkpoints + scripts hôtes..."
docker exec spark-master bash -c "rm -rf /tmp/checkpoint_*"

pkill -f simulateur_capteurs.py 2>/dev/null || true
pkill -f mqtt_to_kafka.py 2>/dev/null || true
nohup python3 simulateur_capteurs.py > /tmp/sim.log 2>&1 &
nohup python3 mqtt_to_kafka.py > /tmp/bridge.log 2>&1 &
echo "  Simulateur et pont MQTT-Kafka lancés."

echo "[6/6] Lancement Spark streaming..."
docker exec -d spark-master bash -c "
  PYTHONPATH=/home/spark/.ivy2/packages /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executorEnv.PYTHONPATH=/home/spark/.ivy2/packages \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\\\
               org.postgresql:postgresql:42.7.1 \
    /app/traitement_kpi.py
"

echo "Vérification finale..."
sleep 8
NB=$(docker exec timescaledb psql -U admin -d iotdb -tAc \
     "SELECT count(*) FROM alertes_predictions WHERE timestamp > now() - interval '30 seconds'" 2>/dev/null || echo "0")
echo "Prédictions reçues (30 dernières secondes) : $NB"

echo ""
echo "Pipeline démarrée. Grafana : http://localhost:3000 (admin/admin)"
