from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit, trim, desc
from pyspark.sql.types import IntegerType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("TransformarPostgres_ImputeMode") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo en S3 (LocalStack)
s3_path_postgres = "s3a://bucket-1/Postgres/postgres_data.csv"

# Leer el archivo CSV con cabecera
df_postgres = spark.read.option("delimiter", ",").csv(s3_path_postgres, header=True)

# Imputación para columnas numéricas con la media (por ejemplo, store_id)
numerical_columns = ['store_id']
for column in numerical_columns:
    mean_value = df_postgres.agg({column: 'mean'}).collect()[0][0]
    df_postgres = df_postgres.na.fill({column: mean_value})

# Columnas a imputar (para datos categóricos)
columns_to_impute = ['store_name', 'location', 'demographics']

for column in columns_to_impute:
    # Calcular el valor más frecuente (modo) para la columna
    mode_row = df_postgres.groupBy(column).count().orderBy(desc("count")).first()
    mode_value = mode_row[column] if (mode_row is not None and mode_row[column] is not None and mode_row[column] != "") else None
    # Si se encontró un valor válido, reemplazar los nulos, cadenas vacías o solo espacios por ese valor
    if mode_value is not None:
        df_postgres = df_postgres.withColumn(
            column,
            when(col(column).isNull() | (trim(col(column)) == ""), lit(mode_value))
            .otherwise(col(column))
        )

# Convertir 'store_id' a entero si es necesario
df_postgres = df_postgres.withColumn("store_id", col("store_id").cast(IntegerType()))

# Agregar columna 'Tratados' y 'Fecha Inserción'
df_postgres = df_postgres.withColumn("Tratados", when(col("store_id").isNotNull(), "Sí").otherwise("No"))
df_postgres = df_postgres.withColumn("Fecha Inserción", current_timestamp())

# Mostrar datos transformados para verificar
df_postgres.show(10, truncate=False)

# Guardar el DataFrame procesado en S3 (sobrescribiendo la carpeta si existe)
output_path_postgres = "s3a://bucket-1/Postgres_limpio/"
df_postgres.write.option("header", "true").mode("overwrite").csv(output_path_postgres)

spark.stop()
