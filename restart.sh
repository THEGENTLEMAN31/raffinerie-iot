#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[1/3] Demarrage des conteneurs..."
docker compose up -d

echo "[2/3] Attente Kafka..."
for i in $(seq 30); do
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list &>/dev/null && break
  sleep 2
done

echo "[3/3] Attente Spark master..."
for i in $(seq 15); do
  curl -s http://localhost:8080 &>/dev/null && break
  sleep 2
done

echo ""
echo "Pipeline demarree."
echo "  Interface web : http://localhost:8081"
echo "  Grafana       : http://localhost:3000 (admin/admin)"
