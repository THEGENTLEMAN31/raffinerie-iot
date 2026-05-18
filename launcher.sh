#!/bin/bash
cd /home/jose/school/projet_de_parcours/raffinerie-iot
source venv/bin/activate
nohup python simulateur_capteurs.py > simulator.out 2>&1 &
echo "Simulateur PID: $!"
nohup python mqtt_to_kafka.py > bridge.out 2>&1 &
echo "Bridge PID: $!"
wait
