import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import time

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sensor-data"

print(f">>> [PONT] Connexion à Kafka sur {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(">>> [PONT] Connexion Kafka OK.")
except Exception as e:
    print(f">>> [PONT] Erreur Kafka : {e}")
    exit(1)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        producer.send(TOPIC_NAME, data)
        producer.flush()
        print(f">>> [PONT] Transmis : {data['type_capteur']} | {data['valeur']}")
    except Exception as e:
        print(f">>> [PONT] Erreur transmission : {e}")

# API v2 obligatoire
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

try:
    print(">>> [PONT] Connexion au broker MQTT (localhost:1883)...")
    client.connect("localhost", 1883)
    client.subscribe("raffinerie/temp")
    client.subscribe("raffinerie/vib")
    client.on_message = on_message
    print(">>> [PONT] Prêt et à l'écoute...")
    client.loop_forever()
except Exception as e:
    print(f">>> [PONT] Erreur MQTT : {e}")
