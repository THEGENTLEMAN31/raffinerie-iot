import time, json, random
import paho.mqtt.client as mqtt
import psycopg2
import os

BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", "1883"))
DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_NAME = os.getenv("DB_NAME", "iotdb")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "60"))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)

def load_machines():
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        cur.execute("SELECT machine_id, target_temp, target_vib, alpha_temp, alpha_vib FROM machine_config WHERE enabled = true")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"DB load error: {e}")
        return [("pipe-101", 95.0, 1.2, 0.05, 0.2), ("pump-303", 95.0, 1.2, 0.05, 0.2)]

machines = load_machines()
print(f">>> [SIMULATEUR] {len(machines)} machine(s) chargee(s) : {[m[0] for m in machines]}")

state = {}
for mid, tt, tv, at, av in machines:
    state[mid] = {
        "temp": tt - 5.0, "vib": tv - 0.1,
        "target_temp": tt, "target_vib": tv,
        "alpha_temp": at, "alpha_vib": av,
        "scenario": "NORMAL", "timer": 0
    }

last_refresh = time.time()

while True:
    now_str = time.strftime('%Y-%m-%dT%H:%M:%SZ')

    if time.time() - last_refresh > REFRESH_INTERVAL:
        machines = load_machines()
        last_refresh = time.time()

    scenario_dice = random.random()
    for mid, tt, tv, at, av in machines:
        s = state[mid]
        if mid not in state:
            state[mid] = {"temp": tt, "vib": tv, "target_temp": tt, "target_vib": tv,
                          "alpha_temp": at, "alpha_vib": av, "scenario": "NORMAL", "timer": 0}
            s = state[mid]

        s["alpha_temp"] = at
        s["alpha_vib"] = av

        if s["timer"] <= 0:
            d = scenario_dice if mid == machines[0][0] else random.random()
            if d < 0.80:
                s["scenario"] = "NORMAL"
                s["target_temp"] = tt
                s["target_vib"] = tv
            elif d < 0.90:
                s["scenario"] = "SURCHAUFFE"
                s["target_temp"] = 145.0
                s["target_vib"] = tv
            else:
                s["scenario"] = "USURE_MECANIQUE"
                s["target_temp"] = tt
                s["target_vib"] = 4.5
            s["timer"] = random.randint(30, 60)

        s["timer"] -= 1

        noise_temp = random.uniform(-0.2, 0.2)
        s["temp"] = (s["temp"] * (1 - s["alpha_temp"])) + (s["target_temp"] * s["alpha_temp"]) + noise_temp
        s["temp"] = max(31, min(149, s["temp"]))

        noise_vib = random.uniform(-0.05, 0.05)
        s["vib"] = (s["vib"] * (1 - s["alpha_vib"])) + (s["target_vib"] * s["alpha_vib"]) + noise_vib
        s["vib"] = max(0.1, min(4.9, s["vib"]))

        msg_temp = json.dumps({"machine_id": mid, "valeur": round(s["temp"], 2), "timestamp": now_str, "type_capteur": "temperature"})
        msg_vib = json.dumps({"machine_id": mid, "valeur": round(s["vib"], 2), "timestamp": now_str, "type_capteur": "vibration"})

        client.publish(f"raffinerie/{mid}/temp", msg_temp)
        client.publish(f"raffinerie/{mid}/vib", msg_vib)

        print(f"[{now_str}] [{mid}] [{s['scenario']}] Temp: {round(s['temp'], 2)}C | Vib: {round(s['vib'], 2)} mm/s")

    time.sleep(2)
