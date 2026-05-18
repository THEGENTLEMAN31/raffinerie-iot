from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, lit
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder \
    .appName("TrainPredictiveModel") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Lecture des données depuis MinIO...")
df = spark.read.json("s3a://raffinerie-raw/training_data/training_data.json")

total = df.count()
print(f"Total enregistrements: {total}")

df.groupBy("label").count().show()

window_spec = Window.partitionBy("machine_id", "type_capteur").orderBy("timestamp")

df = df.withColumn("v_lag1", lag("valeur", 1).over(window_spec))
df = df.withColumn("v_lag2", lag("valeur", 2).over(window_spec))
df = df.withColumn("v_lag3", lag("valeur", 3).over(window_spec))
df = df.withColumn("v_lag4", lag("valeur", 4).over(window_spec))
df = df.withColumn("v_lag5", lag("valeur", 5).over(window_spec))

df = df.withColumn("slope_1", col("valeur") - col("v_lag1"))
df = df.withColumn("slope_3", (col("valeur") - col("v_lag3")) / 3)

df = df.dropna()

indexer = StringIndexer(inputCol="type_capteur", outputCol="sensor_idx")

assembler = VectorAssembler(
    inputCols=[
        "valeur",
        "v_lag1", "v_lag2", "v_lag3", "v_lag4", "v_lag5",
        "slope_1", "slope_3",
        "sensor_idx"
    ],
    outputCol="features"
)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=30,
    maxDepth=6,
    seed=42
)

pipeline = Pipeline(stages=[indexer, assembler, rf])

train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

print(f"Train: {train_data.count()}, Test: {test_data.count()}")

model = pipeline.fit(train_data)

predictions = model.transform(test_data)

evaluator = BinaryClassificationEvaluator(labelCol="label")
auc = evaluator.evaluate(predictions)
print(f"AUC: {auc:.4f}")

model.write().overwrite().save("/app/predictive_model")
print("Modèle sauvegardé dans /app/predictive_model")

spark.stop()
