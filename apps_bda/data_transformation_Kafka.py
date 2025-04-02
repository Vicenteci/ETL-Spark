from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("VerDatosS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo en S3 (LocalStack)
s3_path = "s3a://bucket-1/SalidaKafka/part-00000-.csv"

# Leer el archivo CSV sin la opción "header"
df = spark.read.option("delimiter", ",").csv(s3_path)

# Asignar los nombres de las columnas manualmente
df = df.toDF("timestamp", "store_id", "product_id", "quantity_sold", "revenue")

# Mostrar los primeros 10 registros
df.show(10)

# Mostrar el esquema para verificar que las columnas se asignaron correctamente
df.printSchema()


