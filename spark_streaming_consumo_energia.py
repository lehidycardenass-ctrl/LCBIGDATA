from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ConsumoEnergiaStreaming") \
    .getOrCreate()

# Reducir logs
spark.sparkContext.setLogLevel("WARN")

# Definir esquema de los datos
schema = StructType([
    StructField("id_hogar", IntegerType(), True),
    StructField("dispositivo", StringType(), True),
    StructField("consumo_kwh", FloatType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "consumo_energia") \
    .load()

# Convertir JSON a columnas
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Agrupar consumo por hogar en ventanas de 1 minuto
consumo_total = parsed_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("id_hogar")
    ) \
    .agg(
        sum("consumo_kwh").alias("total_consumo_kwh")
    )

# Mostrar resultados en consola
query = consumo_total \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Mantener ejecución activa
query.awaitTermination()
