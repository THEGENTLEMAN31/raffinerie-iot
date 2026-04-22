import time, json, random
import paho.mqtt.client as mqtt

# --- CONFIGURATION ---
BROKER = "localhost"
PORT = 1883
TOPIC_TEMP = "raffinerie/temp"
TOPIC_VIB = "raffinerie/vib"

# --- PARAMÈTRES PHYSIQUES (Logique Simulation) ---
ALPHA_TEMP = 0.05  # Inertie thermique (plus petit = plus lent)
ALPHA_VIB = 0.2    # Inertie mécanique
TARGET_TEMP = 95.0 # Point de consigne nominal
TARGET_VIB = 1.2   # Point de consigne nominal

# États initiaux
current_temp = 90.0
current_vib = 1.1
current_scenario = "NORMAL"
scenario_timer = 0

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)

print(f">>> [SIMULATEUR] Logique métier activée : Inertie + Scénarios aléatoires.")

while True:
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # 1. GESTION DES SCÉNARIOS (Changement toutes les 30-60 itérations)
    if scenario_timer <= 0:
        dice = random.random()
        if dice < 0.90:
            current_scenario = "NORMAL"
            TARGET_TEMP = 95.0
            TARGET_VIB = 1.2
        elif dice < 0.95:
            current_scenario = "SURCHAUFFE"
            TARGET_TEMP = 145.0  # Proche de la limite Spark (150)
        else:
            current_scenario = "USURE_MECANIQUE"
            TARGET_VIB = 4.5    # Proche de la limite Spark (5)
        scenario_timer = random.randint(30, 60)
    
    scenario_timer -= 1

    # 2. LOGIQUE PHYSIQUE (Lissage Exponentiel + Bruit)
    # Formule : V_t = (V_{t-1} * (1-alpha)) + (Cible * alpha) + bruit
    noise_temp = random.uniform(-0.2, 0.2)
    current_temp = (current_temp * (1 - ALPHA_TEMP)) + (TARGET_TEMP * ALPHA_TEMP) + noise_temp
    
    noise_vib = random.uniform(-0.05, 0.05)
    current_vib = (current_vib * (1 - ALPHA_VIB)) + (TARGET_VIB * ALPHA_VIB) + noise_vib

    # Sécurité pour rester dans les filtres Spark
    current_temp = max(31, min(149, current_temp))
    current_vib = max(0.1, min(4.9, current_vib))

    # 3. ENVOI DES MESSAGES
    msg_temp = json.dumps({
        "machine_id": "pipe-101",
        "valeur": round(current_temp, 2),
        "timestamp": now,
        "type_capteur": "temperature"
    })

    msg_vib = json.dumps({
        "machine_id": "pump-303",
        "valeur": round(current_vib, 2),
        "timestamp": now,
        "type_capteur": "vibration"
    })

    client.publish(TOPIC_TEMP, msg_temp)
    client.publish(TOPIC_VIB, msg_vib)

    print(f"[{now}] [{current_scenario}] Temp: {round(current_temp, 2)}°C | Vib: {round(current_vib, 2)} mm/s")
    
    time.sleep(2)
