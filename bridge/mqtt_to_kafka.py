import paho.mqtt.client as mqtt
from kafka import KafkaProducer, errors
import json
import os
import time
import sys

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "sensor-data")
INTERVAL = float(os.getenv("BRIDGE_INTERVAL", "2.0"))

def wait_for_kafka(broker, retries=30, delay=2):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            p.close()
            print(f">>> [BRIDGE] Kafka ready apres {i+1} tentative(s)")
            return
        except errors.NoBrokersAvailable:
            print(f">>> [BRIDGE] Kafka indisponible, tentative {i+1}/{retries} dans {delay}s...")
            time.sleep(delay)
    print(">>> [BRIDGE] ERREUR: Kafka inaccessible apres {retries} tentatives")
    sys.exit(1)

print(f">>> [BRIDGE] Attente de Kafka {KAFKA_BROKER}...")
wait_for_kafka(KAFKA_BROKER)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        producer.send(TOPIC_NAME, data)
        producer.flush()
        print(f">>> [BRIDGE] {data.get('type_capteur','?')} | {data.get('machine_id','?')} | {data.get('valeur','?')}")
    except Exception as e:
        print(f">>> [BRIDGE] Erreur : {e}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe("raffinerie/+/temp")
client.subscribe("raffinerie/+/vib")
client.on_message = on_message
print(f">>> [BRIDGE] Connecte a MQTT {MQTT_BROKER}:{MQTT_PORT} -> Kafka {KAFKA_BROKER}")
client.loop_forever()
