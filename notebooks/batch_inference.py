"""Batch inference using full_model."""
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import json, time

DATA_PATH = "../dados/chicago_crime.parquet"
MODEL_PATH = "../dados/full_model"
OUT_PARQUET = "../dados/batch_preds.parquet"

spark = SparkSession.builder.appName("Batch_Inference").getOrCreate()
model = PipelineModel.load(MODEL_PATH)

df = spark.read.parquet(DATA_PATH)
t0 = time.time()
pred = model.transform(df)
print(f"Inference time: {time.time()-t0:.1f}s")

pred.select("prediction", "probability").write.mode("overwrite").parquet(OUT_PARQUET)
print("Saved predictions:", OUT_PARQUET)

evaluator = BinaryClassificationEvaluator(labelCol="Arrest", metricName="areaUnderROC")
auc = evaluator.evaluate(pred)
print("AUC:", auc)

spark.stop()
