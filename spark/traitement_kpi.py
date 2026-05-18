from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, expr, to_timestamp, lag, udf, coalesce, lit
from pyspark.sql.types import StructType, StringType, FloatType, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import Window
import psycopg2

# 1. Définir le schéma attendu pour les messages JSON
schema = StructType() \
    .add("machine_id", StringType()) \
    .add("valeur", FloatType()) \
    .add("timestamp", StringType()) \
    .add("type_capteur", StringType())

# 2. Démarrer la session Spark
spark = SparkSession.builder \
    .appName("raffinerie-iot") \
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

# 7. Filtrer les données valides
filtrees = json_df.filter(
    ((col("type_capteur") == "temperature") & (col("valeur").between(30, 150))) |
    ((col("type_capteur") == "vibration") & (col("valeur").between(0, 5)))
)

# 8. Fonction batch pour enregistrer les mesures filtrées dans TimescaleDB
def save_filtrees_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "mesures_filtrees") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 9. Écriture en base TimescaleDB des mesures filtrées
filtrees.writeStream \
    .foreachBatch(save_filtrees_to_pg) \
    .option("checkpointLocation", "/tmp/checkpoint_filtrees") \
    .outputMode("append") \
    .start()

# 10. Calcul des KPI : moyenne glissante sur 1 minute
kpi = filtrees.withColumn("ts", col("timestamp")) \
    .withWatermark("ts", "30 seconds") \
    .groupBy(window("ts", "1 minute"), "type_capteur") \
    .agg(avg("valeur").alias("valeur")) \
    .withColumn("type_kpi", col("type_capteur")) \
    .withColumn("unite", expr("CASE WHEN type_capteur = 'temperature' THEN '°C' ELSE 'mm/s' END")) \
    .selectExpr("window.start as timestamp", "type_kpi", "valeur", "unite")

# 11. Fonction batch pour enregistrer les KPI dans TimescaleDB
def save_kpi_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "kpi_indicateurs") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 12. Écriture en base TimescaleDB des KPI
kpi.writeStream \
    .foreachBatch(save_kpi_to_pg) \
    .option("checkpointLocation", "/tmp/checkpoint_kpi") \
    .outputMode("append") \
    .start()

# 13. Chargement du modèle prédictif
print("Chargement du modèle prédictif...")
model = PipelineModel.load("/home/spark/.ivy2/predictive_model")
print("Modèle chargé.")

def load_seuil():
    try:
        conn = psycopg2.connect(host="timescaledb", dbname="iotdb", user="admin", password="admin")
        cur = conn.cursor()
        cur.execute("SELECT seuil_anomalie FROM alert_config WHERE machine_id IS NULL AND type_capteur IS NULL LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return float(row[0]) if row else 0.7
    except:
        return 0.7

SEUIL_ALERTE = load_seuil()
print(f"Seuil d'alerte charge depuis la base : {SEUIL_ALERTE}")

extract_prob = udf(lambda v: float(v[1]) if v else 0.0, DoubleType())

# 14. Fonction d'inférence et d'alerte
def predict_and_alert(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    window_spec = Window.partitionBy("machine_id", "type_capteur").orderBy("timestamp")

    pred_df = batch_df \
        .withColumn("v_lag1", lag("valeur", 1).over(window_spec)) \
        .withColumn("v_lag2", lag("valeur", 2).over(window_spec)) \
        .withColumn("v_lag3", lag("valeur", 3).over(window_spec)) \
        .withColumn("v_lag4", lag("valeur", 4).over(window_spec)) \
        .withColumn("v_lag5", lag("valeur", 5).over(window_spec))

    pred_df = pred_df \
        .withColumn("v_lag1", coalesce(col("v_lag1"), col("valeur"))) \
        .withColumn("v_lag2", coalesce(col("v_lag2"), col("valeur"))) \
        .withColumn("v_lag3", coalesce(col("v_lag3"), col("valeur"))) \
        .withColumn("v_lag4", coalesce(col("v_lag4"), col("valeur"))) \
        .withColumn("v_lag5", coalesce(col("v_lag5"), col("valeur")))

    pred_df = pred_df \
        .withColumn("slope_1", col("valeur") - col("v_lag1")) \
        .withColumn("slope_3", (col("valeur") - col("v_lag3")) / 3)

    predictions = model.transform(pred_df)
    predictions = predictions.withColumn("proba", extract_prob(col("probability")))

    predictions.select(
        col("timestamp"),
        col("machine_id"),
        col("type_capteur"),
        col("valeur"),
        col("prediction"),
        col("proba").alias("score_anomalie")
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "alertes_predictions") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 15. Inférence temps réel sur le flux filtré
filtrees.writeStream \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(predict_and_alert) \
    .option("checkpointLocation", "/tmp/checkpoint_inference") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
