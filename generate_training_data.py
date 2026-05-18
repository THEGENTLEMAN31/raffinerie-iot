import json
import random
from datetime import datetime, timedelta
from minio import Minio

# --- PARAMÈTRES PHYSIQUES (identique au simulateur) ---
ALPHA_TEMP = 0.05
ALPHA_VIB = 0.2
NOISE_TEMP_RANGE = 0.2
NOISE_VIB_RANGE = 0.05

# --- CONFIG MINIO ---
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "raffinerie-raw"
TRAINING_PREFIX = "training_data"
NUM_ITERATIONS = 5000


def simulate():
    current_temp = 90.0
    current_vib = 1.1
    target_temp = 95.0
    target_vib = 1.2
    scenario = "NORMAL"
    timer = 0
    records = []
    start_time = datetime.utcnow() - timedelta(seconds=NUM_ITERATIONS * 2)

    for i in range(NUM_ITERATIONS):
        now = (start_time + timedelta(seconds=i * 2)).strftime('%Y-%m-%dT%H:%M:%SZ')

        if timer <= 0:
            dice = random.random()
            if dice < 0.90:
                scenario = "NORMAL"
                target_temp = 95.0
                target_vib = 1.2
            elif dice < 0.95:
                scenario = "SURCHAUFFE"
                target_temp = 145.0
                target_vib = 1.2
            else:
                scenario = "USURE_MECANIQUE"
                target_vib = 4.5
                target_temp = 95.0
            timer = random.randint(30, 60)

        timer -= 1

        noise_temp = random.uniform(-NOISE_TEMP_RANGE, NOISE_TEMP_RANGE)
        current_temp = (current_temp * (1 - ALPHA_TEMP)) + (target_temp * ALPHA_TEMP) + noise_temp
        current_temp = max(31, min(149, current_temp))

        noise_vib = random.uniform(-NOISE_VIB_RANGE, NOISE_VIB_RANGE)
        current_vib = (current_vib * (1 - ALPHA_VIB)) + (target_vib * ALPHA_VIB) + noise_vib
        current_vib = max(0.1, min(4.9, current_vib))

        label = 1 if scenario != "NORMAL" else 0

        records.append({
            "machine_id": "pipe-101",
            "valeur": round(current_temp, 2),
            "timestamp": now,
            "type_capteur": "temperature",
            "label": label
        })
        records.append({
            "machine_id": "pump-303",
            "valeur": round(current_vib, 2),
            "timestamp": now,
            "type_capteur": "vibration",
            "label": label
        })

    return records


def upload_to_minio(local_path):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' créé")

    dest = f"{TRAINING_PREFIX}/training_data.json"
    client.fput_object(BUCKET_NAME, dest, local_path)
    print(f"Uploadé vers MinIO : {BUCKET_NAME}/{dest}")


if __name__ == "__main__":
    print(f"Génération de {NUM_ITERATIONS} itérations ({NUM_ITERATIONS * 2} enregistrements)...")
    data = simulate()

    local_path = "/tmp/training_data.json"
    with open(local_path, "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")
    print(f"Fichier local créé : {local_path}")

    upload_to_minio(local_path)
    print("Terminé.")
