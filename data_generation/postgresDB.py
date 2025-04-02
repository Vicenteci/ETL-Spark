import psycopg2
import random
import string
from faker import Faker

fake = Faker()

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def create_table():
    try:
        # Establecer la conexión con la base de datos
        conn = psycopg2.connect(
            host="localhost",
            database="retail_db",
            user="postgres",
            password="casa1234",
            port=5432
        )
        
        # Crear la tabla si no existe
        createTableString = '''
        CREATE TABLE IF NOT EXISTS Stores (
            store_id SERIAL PRIMARY KEY,
            store_name VARCHAR(255),
            location VARCHAR(255),
            demographics VARCHAR(255)
        );
        '''
        
        with conn.cursor() as cur:
            cur.execute(createTableString)
        conn.commit()
        
        # Insertar datos con errores controlados
        with conn.cursor() as cur:
            for _ in range(1000):
                # Ajustamos las probabilidades de los valores erróneos
                store_name = random.choice([fake.company(), get_random_string(10)])  # Menos vacíos y nulos
                location = random.choice([fake.city(), "Unknown123"])  # Menos nulos
                demographics = random.choice([fake.text(20), "###"])  # Eliminamos vacíos y nulos
                
                # Insertar datos en la tabla
                insertString = """ 
                    INSERT INTO Stores (store_name, location, demographics) 
                    VALUES (%s, %s, %s) 
                """
                cur.execute(insertString, (store_name, location, demographics))
            conn.commit()

    except (psycopg2.DatabaseError, Exception) as error:
        print("Error: ", error)
    finally:
        if conn:
            conn.close()

# Ejecutar la función para crear la tabla e insertar los datos
if __name__ == '__main__':
    create_table()
