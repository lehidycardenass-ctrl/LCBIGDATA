from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

spark = SparkSession.builder.appName("BatchConsumoEnergia").getOrCreate()

# Simulación de datos (puedes usar CSV si quieres)
data = [
    (1, "Nevera", 1.5),
    (1, "Televisor", 0.8),
    (2, "Aire acondicionado", 3.2),
    (2, "Lavadora", 2.1),
    (3, "Computador", 1.0)
]

columns = ["id_hogar", "dispositivo", "consumo_kwh"]

df = spark.createDataFrame(data, columns)

df.show()

# Análisis batch
df.groupBy("id_hogar").avg("consumo_kwh").show()
df.groupBy("id_hogar").max("consumo_kwh").show()
df.groupBy("id_hogar").min("consumo_kwh").show()
