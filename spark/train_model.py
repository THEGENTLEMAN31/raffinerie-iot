from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("TrainPredictiveModel") \
    .getOrCreate()

# 2. Load the historical data
# Note: In a real scenario, we would read from MinIO (s3a://raffinerie-raw/raw)
# For this mission, we read from the generated local file in /app
df = spark.read.json("/app/historical_data.json")

# 3. Preprocessing: Sliding window of 5 values
# We need to sort by timestamp for each machine
window_spec = Window.partitionBy("machine_id").orderBy("timestamp")

df_lagged = df.withColumn("v_lag1", lag("valeur", 1).over(window_spec)) \
              .withColumn("v_lag2", lag("valeur", 2).over(window_spec)) \
              .withColumn("v_lag3", lag("valeur", 3).over(window_spec)) \
              .withColumn("v_lag4", lag("valeur", 4).over(window_spec))

# Drop rows with nulls (the first 4 rows per machine)
df_clean = df_lagged.dropna()

# 4. Feature Engineering
# Encode type_capteur
indexer = StringIndexer(inputCol="type_capteur", outputCol="sensor_idx")

# Assemble features: current value + 4 previous values + sensor type
assembler = VectorAssembler(
    inputCols=["valeur", "v_lag1", "v_lag2", "v_lag3", "v_lag4", "sensor_idx"],
    outputCol="features"
)

# 5. Model Definition
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)

# Pipeline
pipeline = Pipeline(stages=[indexer, assembler, rf])

# 6. Train/Test Split
train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)

# 7. Training
model = pipeline.fit(train_data)

# 8. Evaluation
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="label")
auc = evaluator.evaluate(predictions)
print(f"Model AUC: {auc}")

# 9. Save the model for later use in the streaming pipeline
model.write().overwrite().save("/app/predictive_model")
print("Model saved to /app/predictive_model")

spark.stop()
