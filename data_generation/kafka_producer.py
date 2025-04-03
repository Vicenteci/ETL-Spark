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
        "timestamp": int(datetime.now().timestamp() * 1000), # Genera un timestamp real en milisegundos
        "store_id": random.randint(1, 100),  # Siempre tiene un valor
        "product_id": fake.uuid4(),  # Siempre tiene un valor
        "quantity_sold": random.choice([random.randint(1, 20), None]),  # 10% de nulos
        "revenue": random.choice([round(random.uniform(100.0, 1000.0), 2), None]),  # 10% de nulos
    }

    producer.send('sales_stream', value=message)
    sleep(1)
