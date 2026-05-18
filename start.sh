#!/bin/bash
cd /home/jose/school/projet_de_parcours/raffinerie-iot
nohup venv/bin/python -u simulateur_capteurs.py > simulator.out 2>&1 &
echo "SIM=$!"
nohup venv/bin/python -u mqtt_to_kafka.py > bridge.out 2>&1 &
echo "BRIDGE=$!"
echo "Done"
