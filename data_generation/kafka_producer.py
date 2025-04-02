from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    message = {
    "timestamp": int(datetime.now().timestamp() * 1000) if random.random() > 0.1 else None,  # 10% de probabilidad de ser nulo
    "store_id": random.choice([random.randint(1, 100), None]),  # 10% de probabilidad de ser nulo
    "product_id": random.choice([fake.uuid4(), None]),  # 10% de probabilidad de ser nulo
    "quantity_sold": random.choice([random.randint(1, 20), -5]),  # 10% de probabilidad de ser negativo
    "revenue": random.choice([round(random.uniform(100.0, 1000.0), 2), "error"]),  # 5% de probabilidad de ser error
}

    producer.send('sales_stream', value=message)
    sleep(1)
