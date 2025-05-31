"""Limpeza + Feature Engineering (+PCA) do Chicago Crime dataset.
Grava parquet pronto para ML e pipeline_fe (inclui PCA).
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import hour, dayofweek, to_timestamp, col
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, Imputer, PCA
)
from pyspark.ml import Pipeline
import os

INPUT_RAW  = "../dados/chicago_crime.parquet"
OUTPUT_PARQUET = "../dados/chicago_ready.parquet"
PIPELINE_PATH  = "../dados/pipeline_fe"

spark = (SparkSession.builder
         .appName("Preprocess_FE")
         .getOrCreate())

df = spark.read.parquet(INPUT_RAW)

# Fix timestamp
df = df.withColumn("Date", to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))
df = df.filter(col("Date").isNotNull())  # drop few nulls

# Derive temporal features
df = df.withColumn("Hour", hour("Date")).withColumn("DayOfWeek", dayofweek("Date"))

# Column lists
cat_cols = ["Primary_Type", "Location_Description"]
num_cols = ["Beat", "District", "Latitude", "Longitude"]
derived_cols = ["Hour", "DayOfWeek"]

# Index + OHE categorical
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in cat_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
            for c in cat_cols]

# Impute numeric
imputer = Imputer(inputCols=num_cols,
                  outputCols=[f"{c}_imp" for c in num_cols])

# Assemble
assembler = VectorAssembler(
    inputCols=[f"{c}_vec" for c in cat_cols] +
              [f"{c}_imp" for c in num_cols] + derived_cols,
    outputCol="features"
)

# PCA
pca = PCA(k=50, inputCol="features", outputCol="pcaFeatures")

pipeline = Pipeline(stages=indexers + encoders + [imputer, assembler, pca])
model_fe = pipeline.fit(df)

df_ready = (model_fe.transform(df)
            .withColumn("label", col("Arrest").cast("int"))
            .select("pcaFeatures", "label")  # rename later
            )

df_ready = df_ready.withColumnRenamed("pcaFeatures", "features")
df_ready.write.mode("overwrite").parquet(OUTPUT_PARQUET)
model_fe.write().overwrite().save(PIPELINE_PATH)

print("✅ Saved dataset:", OUTPUT_PARQUET)
print("✅ Saved pipeline_fe:", PIPELINE_PATH)
spark.stop()
