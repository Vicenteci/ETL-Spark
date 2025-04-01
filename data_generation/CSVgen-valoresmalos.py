"""#pip install fastparquet
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



def generate_text_file(file_name, num_lines=100):
    with open(f"{output_dir}/{file_name}", 'w') as f:
        for _ in range(num_lines):
            f.write(f"{fake.sentence()}\n")

Def generate_csv_file(file_name, num_rows=100):
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

def generate_json_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        product_id = fake.uuid4()
        category = fake.word()
        price = round(random.uniform(5.0, 1000.0), 2)
        data.append({"product_id": product_id, "category": category, "price": price})
    with open(f"{output_dir}/{file_name}", 'w') as f:
        json.dump(data, f, indent=4)

def generate_parquet_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        product_id = fake.uuid4()
        category = fake.word()
        price = round(random.uniform(5.0, 1000.0), 2)
        data.append({"product_id": product_id, "category": category, "price": price})
    df = pd.DataFrame(data)
    df.to_parquet(f"{output_dir}/{file_name}", index=False)

def generate_order_data(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        customer_id = fake.uuid4()
        product_id = fake.uuid4()
        quantity = random.randint(1, 5)
        total_price = round(random.uniform(10.0, 500.0), 2)
        data.append([customer_id, product_id, quantity, total_price])
    df = pd.DataFrame(data, columns=['customer_id', 'product_id', 'quantity', 'total_price'])
    df.to_csv(f"{output_dir}/{file_name}", index=False)
    print(f"{output_dir}/{file_name}")

# Generate required files
generate_text_file("text_data.txt", 200)
generate_csv_file("sales_data.csv", 2000)
generate_json_file("sales_data.json", 150)
generate_parquet_file("filtered_products.parquet", 100)
generate_order_data("purchases.csv", 150)
"""



# ESTO ES PARA GENERAR EL MISMO CSV 
# PERO CON DATOS ERRONEOS COMO PEDIA EN LA PRACTICA

import os
import random
import pandas as pd
import json
from faker import Faker
from pathlib import Path

# Definir la ruta de salida
source_path = Path(__file__).resolve()
output_dir = source_path.parent
os.makedirs(output_dir, exist_ok=True)

# Inicializar la instancia de Faker
fake = Faker()

def generate_csv_file(file_name="sales_data.csv", num_rows=100):
    data = []
    
    for _ in range(num_rows):
        date = fake.date()
        store_id = random.choice([fake.random_int(min=1, max=1000), "", None])  # Algunos valores nulos o vacíos
        product_id = random.choice([f"P{random.randint(18, 80)}", "XX", None])  # Alfanumérico con errores
        quantity_sold = random.choice([random.randint(20000, 120000), "", None, -5])  # Valores erróneos
        revenue = random.choice([random.randint(100, 3000), "", None, "error"])  # Errores de formato
        
        data.append([date, store_id, product_id, quantity_sold, revenue])
    
    df = pd.DataFrame(data, columns=['date', 'store_id', 'product_id', 'quantity_sold', 'revenue'])
    
    file_path = os.path.join(output_dir, file_name)
    df.to_csv(file_path, index=False)
    print(f"Archivo generado: {file_path}")

# Ejecutar la función para generar el archivo
generate_csv_file("sales_data.csv", 2000)
