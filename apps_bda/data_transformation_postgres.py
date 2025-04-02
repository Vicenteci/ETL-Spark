from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, to_date, current_timestamp, lit

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("DataLake Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Parámetros de configuración
bucket_name = 'bucket-1'
file_key = 'Postgres/postgres_data.csv'

# Cargar el archivo CSV desde S3
df = spark.read.csv(f"s3a://{bucket_name}/{file_key}", header=True, inferSchema=True)

# Mostrar las primeras filas para verificar la carga de datos
df.show()

# ================================
# Tratamiento de valores perdidos (Missing Values)
# ================================

# Imputación: Rellenar valores nulos con la media de la columna 'quantity_sold'
mean_value = df.select(mean(col('quantity_sold'))).collect()[0][0]
df = df.fillna({'quantity_sold': mean_value})

# Suprimir filas con valores nulos (si lo deseas)
# df = df.dropna()

# Suprimir valores nulos en columnas específicas
# df = df.dropna(subset=['store_id', 'product_id'])

# ================================
# Eliminación de duplicados
# ================================

# Eliminar duplicados basados en las columnas 'store_id' y 'product_id'
df = df.dropDuplicates(['store_id', 'product_id'])

# ================================
# Conversión de tipos de datos
# ================================

# Convertir 'quantity_sold' a tipo entero
df = df.withColumn("quantity_sold", df["quantity_sold"].cast("int"))

# Convertir la columna 'date' a tipo Date
df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))

# ================================
# Detección y tratamiento de valores atípicos
# ================================

# Calcular la media y desviación estándar de la columna 'quantity_sold'
mean_value = df.select(mean(col('quantity_sold'))).collect()[0][0]
stddev_value = df.select(stddev(col('quantity_sold'))).collect()[0][0]

# Definir un umbral para detectar outliers (por ejemplo, 3 desviaciones estándar)
threshold = 3

# Filtrar los valores que están más allá de 3 desviaciones estándar de la media
df = df.filter((col('quantity_sold') < mean_value + threshold * stddev_value) &
               (col('quantity_sold') > mean_value - threshold * stddev_value))

# O también puedes usar percentiles para detectar outliers:
quantiles = df.approxQuantile("quantity_sold", [0.01, 0.99], 0.0)

# Filtrar los valores atípicos (por debajo del 1% y por encima del 99%)
df = df.filter((col("quantity_sold") > quantiles[0]) & (col("quantity_sold") < quantiles[1]))

# ================================
# Agregar nuevas columnas
# ================================

# Agregar la columna 'Tratados' y 'Fecha Inserción'
df = df.withColumn("Tratados", lit("Sí")) \
       .withColumn("Fecha Inserción", current_timestamp())

# Mostrar las primeras filas para verificar
df.show()

# ================================
# Guardar los datos transformados en formato Parquet
# ================================

output_path = "s3a://bucket-1/Postgres/transformed_data.parquet"
df.write.parquet(output_path)

print(f"Datos transformados guardados en: {output_path}")
