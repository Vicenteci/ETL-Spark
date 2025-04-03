from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker
import time
import random

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    message = {
        "timestamp": random.randint(10000000000, 10000000000000) * 1000,# Siempre tiene un valor (sin posibilidad de ser nulo)
        "store_id": random.randint(1, 100),  # Siempre tiene un valor (sin posibilidad de ser nulo)
        "product_id": fake.uuid4(),  # Siempre tiene un valor (sin posibilidad de ser nulo)
        "quantity_sold": random.choice([random.randint(1, 20), None]),  # 10% de probabilidad de ser nulo
        "revenue": random.choice([round(random.uniform(100.0, 1000.0), 2), None]),  # 10% de probabilidad de ser nulo
    }

    producer.send('sales_stream', value=message)
    sleep(1)
