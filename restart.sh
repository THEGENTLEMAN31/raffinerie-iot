#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[1/3] Demarrage des conteneurs Docker..."
docker compose up -d

echo "[2/3] Attente de Kafka..."
for i in $(seq 30); do
  docker exec kafka bash -c "kafka-topics --bootstrap-server localhost:29092 --list" &>/dev/null && break
  sleep 2
done

echo "[3/3] Attente de Spark master..."
for i in $(seq 15); do
  docker exec spark-master bash -c "curl -s http://localhost:8080" &>/dev/null && break
  sleep 2
done

echo "Verification finale..."
sleep 10
NB=$(docker exec timescaledb psql -U admin -d iotdb -tAc \
     "SELECT count(*) FROM alertes_predictions WHERE timestamp > now() - interval '30 seconds'" 2>/dev/null || echo "0")
echo "Predictions recues (30 dernieres secondes) : $NB"

echo ""
echo "Pipeline demarree."
echo "  Interface web : http://localhost:8080"
echo "  Grafana       : http://localhost:3000 (admin/admin)"
