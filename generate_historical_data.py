import json
import random
from datetime import datetime, timedelta

def generate_mock_data(n_iterations=5000):
    # Initial states
    current_temp = 150.0
    current_vib = 2.1
    ALPHA_TEMP = 0.05
    ALPHA_VIB = 0.2
    
    current_scenario = 0
    scenario_timer = 0
    
    # Start time (approx 2.5 hours ago if n=5000 and interval=2s)
    start_time = datetime.now() - timedelta(seconds=n_iterations * 2)
    
    data = []
    
    for i in range(n_iterations):
        # Scenario management
        if scenario_timer <= 0:
            dice = random.random()
            if dice < 0.85:
                current_scenario = 0 # Normal
            elif dice < 0.95:
                current_scenario = 1 # Temp Drift
            else:
                current_scenario = 2 # Vib Drift
            scenario_timer = random.randint(30, 60)
        
        scenario_timer -= 1
        
        # Temp (Pipe-101)
        target_temp = 150.0
        if current_scenario == 1:
            target_temp = 185.0 + random.uniform(-2, 2)
        noise_temp = random.uniform(-0.1, 0.1)
        current_temp = (current_temp * (1 - ALPHA_TEMP)) + (target_temp * ALPHA_TEMP) + noise_temp
        
        # Vib (Pump-303)
        target_vib = 2.1
        if current_scenario == 2:
            target_vib = 4.5 + random.uniform(-0.5, 0.5)
        noise_vib = random.uniform(-0.05, 0.05)
        current_vib = (current_vib * (1 - ALPHA_VIB)) + (target_vib * ALPHA_VIB) + noise_vib
        
        ts = (start_time + timedelta(seconds=i * 2)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # We save both in the same "raw" format
        data.append({
            "machine_id": "pipe-101",
            "valeur": round(current_temp, 2),
            "timestamp": ts,
            "type_capteur": "temperature",
            "label": 1 if current_scenario == 1 else 0  # Useful for validation but hidden in "raw"
        })
        data.append({
            "machine_id": "pump-303",
            "valeur": round(current_vib, 2),
            "timestamp": ts,
            "type_capteur": "vibration",
            "label": 1 if current_scenario == 2 else 0
        })
        
    return data

if __name__ == "__main__":
    print("Generating historical data...")
    dataset = generate_mock_data(10000) # 20,000 records total
    with open("raffinerie-iot/spark/historical_data.json", "w") as f:
        for item in dataset:
            f.write(json.dumps(item) + "\n")
    print(f"Historical data generated in raffinerie-iot/spark/historical_data.json")
