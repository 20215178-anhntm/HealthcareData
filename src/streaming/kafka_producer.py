# mypy: ignore-errors
import json, time, random
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    event = {
        "patient_id": random.randint(1000, 9999),
        "name": random.choice(["John Doe", "Jane Smith", "Alice Nguyen", "Bob Tran", "David Lee"]),
        "age": random.randint(18, 80),
        "gender": random.choice(["Male", "Female"]),
        "pregnancies": random.randint(0, 10) if random.random() < 0.5 else None,
        "glucose": random.randint(70, 200),
        "blood_pressure": random.randint(60, 120),
        "skin_thickness": random.randint(10, 50),
        "insulin": random.randint(15, 276),
        "bmi": round(random.uniform(18.0, 40.0), 2),
        "diabetes_pedigree": round(random.uniform(0.1, 2.5), 3),
        "outcome": random.choice([0, 1]),  # 1 = diabetic, 0 = normal
        "insurance_type": random.choice(["Basic", "Premium", "Gold"]),
        "visit_cost": round(random.uniform(50.0, 500.0), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send(KAFKA_TOPIC, event)
    print("Sent:", event)
    time.sleep(2)
