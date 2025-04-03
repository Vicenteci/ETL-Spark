import psycopg2
import random
import string
from faker import Faker

fake = Faker()

conn = None

def create_table():
    global conn
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="retail_db",
            user="postgres",
            password="casa1234",
            port=5432
        )
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

        with conn.cursor() as cur:
            for _ in range(1000):
                store_name = fake.company() if random.random() > 0.1 else None
                location = fake.city() if random.random() > 0.1 else None
                demographics = fake.text(20) if random.random() > 0.1 else None
                
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

if __name__ == '__main__':
    create_table()