from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

# 1. Définir le schéma attendu pour les messages JSON
schema = StructType() \
    .add("machine_id", StringType()) \
    .add("valeur", FloatType()) \
    .add("timestamp", StringType()) \
    .add("type_capteur", StringType())

# 2. Démarrer la session Spark
spark = SparkSession.builder \
    .appName("raffinerie-iot-debug") \
    .getOrCreate()

# 3. Lire les données en streaming depuis Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Décoder les messages JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Convertir correctement le champ timestamp
json_df = json_df.withColumn("timestamp", to_timestamp("timestamp"))

# 6. Afficher en console (DEBUG)
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
