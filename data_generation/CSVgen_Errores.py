import os
import random
import pandas as pd
from faker import Faker
from pathlib import Path

# Inicialización de Faker
fake = Faker()

# Ruta de salida
source_path = Path(__file__).resolve()
output_dir = source_path.parent

def generate_csv_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        date = fake.date() if random.random() > 0.1 else None  # 10% de nulos
        store_id = random.randint(1, 1000) if random.random() > 0.1 else None  # 10% de nulos
        product_id = f"P{random.randint(18, 80)}" if random.random() > 0.1 else None  # 10% de nulos
        quantity_sold = random.randint(20000, 120000) if random.random() > 0.1 else "error"  # 10% de error
        revenue = round(random.uniform(100, 3000), 2) if random.random() > 0.1 else "error"  # 10% de error
        
        data.append([date, store_id, product_id, quantity_sold, revenue])
    
    df = pd.DataFrame(data, columns=['date', 'store_id', 'product_id', 'quantity_sold', 'revenue'])
    df.to_csv(output_dir / file_name, index=False)

generate_csv_file("sales_data.csv", 2000)