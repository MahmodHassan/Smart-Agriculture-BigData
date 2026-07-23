import pandas as pd
import numpy as np
import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer
# ==========================================
# Configuration
# ==========================================

DATASET_PATH = "plant_health_data.csv"

NUM_PLANTS = 20
DELAY = 1              # ثانية بين كل قراءة
ANOMALY_RATE = 0.01    # 1%

# ==========================================
# Kafka Producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# ==========================================
# Load Dataset
# ==========================================

try:
    df = pd.read_csv(DATASET_PATH)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])

    print("=" * 60)
    print("Dataset Loaded Successfully")
    print(f"Rows Loaded: {len(df)}")
    print("=" * 60)

except Exception as e:
    print(f"Error loading dataset: {e}")
    exit()

current_index = 0

# ==========================================
# Infinite Simulation
# ==========================================

while True:

    row = df.iloc[current_index].copy()

    current_index += 1

    if current_index >= len(df):
        current_index = 0

    # Update Timestamp
    row["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Random Plant ID
    row["Plant_ID"] = f"Plant_{random.randint(1, NUM_PLANTS):03}"

    # Sensor Noise
    row["Soil_Moisture"] += np.random.normal(0, 1)
    row["Ambient_Temperature"] += np.random.normal(0, 0.3)
    row["Soil_Temperature"] += np.random.normal(0, 0.3)
    row["Humidity"] += np.random.normal(0, 0.8)
    row["Light_Intensity"] += np.random.normal(0, 15)
    row["Soil_pH"] += np.random.normal(0, 0.03)
    row["Nitrogen_Level"] += np.random.normal(0, 1)
    row["Phosphorus_Level"] += np.random.normal(0, 1)
    row["Potassium_Level"] += np.random.normal(0, 1)
    row["Chlorophyll_Content"] += np.random.normal(0, 0.1)
    row["Electrochemical_Signal"] += np.random.normal(0, 0.02)

    # Anomaly Injection
    if random.random() < ANOMALY_RATE:

        anomaly = random.choice([
            "temperature",
            "humidity",
            "soil",
            "light",
            "ph"
        ])

        if anomaly == "temperature":
            row["Ambient_Temperature"] = random.choice([90, -20])

        elif anomaly == "humidity":
            row["Humidity"] = random.choice([150, -15])

        elif anomaly == "soil":
            row["Soil_Moisture"] = random.choice([-10, 180])

        elif anomaly == "light":
            row["Light_Intensity"] = random.choice([-1000, 150000])

        elif anomaly == "ph":
            row["Soil_pH"] = random.choice([-2, 18])

    # Round Values
    for col in [
        "Soil_Moisture",
        "Ambient_Temperature",
        "Soil_Temperature",
        "Humidity",
        "Light_Intensity",
        "Soil_pH",
        "Nitrogen_Level",
        "Phosphorus_Level",
        "Potassium_Level",
        "Chlorophyll_Content",
        "Electrochemical_Signal"
    ]:
        row[col] = round(float(row[col]), 2)
    # Convert to Dictionary
    sensor_data = row.to_dict()

    producer.send("plant-sensors", sensor_data)
    producer.flush()

    print(f"Sent -> {sensor_data['Plant_ID']} | {sensor_data['Timestamp']}")

    time.sleep(DELAY)
