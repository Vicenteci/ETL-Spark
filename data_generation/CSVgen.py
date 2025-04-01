#pip install fastparquet
import os
import random
import pandas as pd
import json
from faker import Faker
from pathlib import Path

source_path = Path(__file__).resolve()
output_dir = source_path.parent
os.makedirs(output_dir, exist_ok=True)

# Initialize the Faker instance
fake = Faker()

def generate_csv_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        date = fake.date()
        store_id = fake.random_int(min=1, max=1000)  # ID de tienda numérico válido
        product_id = f"P{random.randint(18, 80)}"  # ID de producto alfanumérico válido
        quantity_sold = random.randint(20000, 120000)  # Cantidad vendida numérica válida
        revenue = random.randint(100, 3000)  # Ingresos numéricos válidos
        data.append([date, store_id, product_id, quantity_sold, revenue])
    df = pd.DataFrame(data, columns=['date', 'store_id', 'product_id', 'quantity_sold', 'revenue'])
    df.to_csv(f"{output_dir}/{file_name}", index=False)


generate_csv_file("sales_data.csv", 2000)