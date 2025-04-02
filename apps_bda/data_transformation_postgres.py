# Postgres

from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("LeerCSVDesdeS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo CSV en S3 (LocalStack)
s3_path = "s3a://bucket-1/Postgres/postgres_data"

# Leer el archivo CSV con PySpark (header=True para usar la primera fila como nombres de columnas)
df = spark.read.option("header", "true").csv(s3_path)

# Mostrar las primeras 10 filas del archivo CSV
df.show(10)
