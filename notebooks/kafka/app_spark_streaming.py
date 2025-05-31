"""Spark Structured Streaming:

1. Lê eventos de crimes (JSON) a partir de Kafka
2. Converte para DataFrame com schema correto
3. Deriva Hour, DayOfWeek e aplica Pipeline FE salvo em disco
4. Aplica modelo salvo (melhor classificador)
5. Escreve previsões em outro tópico Kafka OU consola
"""

import argparse, os, json
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel

################################################################################
# argumentos CLI
################################################################################

p = argparse.ArgumentParser()
p.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
p.add_argument("--subscribe", default=os.getenv("KAFKA_TOPIC_IN", "chicago_crimes"))
p.add_argument("--output_topic", default=os.getenv("KAFKA_TOPIC_OUT", "crime_predictions"))
p.add_argument("--pipeline", default=os.getenv("PIPE_FE_DIR", "../dados/pipeline_fe"),
               help="Diretório do Pipeline FE salvo no notebook 03")
p.add_argument("--model", default=os.getenv("MODEL_DIR", "../dados/best_model"),
               help="Diretório do melhor modelo salvo no notebook 04")
p.add_argument("--checkpoint", default="/tmp/spark-checkpoint-crime")
p.add_argument("--console", action="store_true", help="Escreve no console em vez de Kafka")
args = p.parse_args()

################################################################################
# Spark session
################################################################################

spark = (SparkSession.builder
         .appName("Crime_Streaming_Inference")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

################################################################################
# Schema para desserializar JSON
################################################################################

input_schema = T.StructType([
    T.StructField("Date", T.StringType()),
    T.StructField("Primary_Type", T.StringType()),
    T.StructField("Location_Description", T.StringType()),
    T.StructField("Beat", T.IntegerType()),
    T.StructField("District", T.IntegerType()),
    T.StructField("Latitude", T.DoubleType()),
    T.StructField("Longitude", T.DoubleType()),
])

################################################################################
# Ler de Kafka
################################################################################

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", args.brokers)
       .option("subscribe", args.subscribe)
       .option("startingOffsets", "latest")
       .load())

df = (raw
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(F.from_json("json_str", input_schema).alias("data"))
      .select("data.*"))

# Converte Date -> timestamp e deriva Hour / DayOfWeek
df = (df
      .withColumn("Date", F.to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))
      .withColumn("Hour", F.hour("Date"))
      .withColumn("DayOfWeek", F.dayofweek("Date")))

################################################################################
# Carregar pipeline de FE e modelo
################################################################################

pipe_fe = PipelineModel.load(args.pipeline)
classifier = PipelineModel.load(args.model)

fe_df = pipe_fe.transform(df)
pred_df = classifier.transform(fe_df)

out_df = pred_df.select(
    F.to_json(
        F.struct(
            "Primary_Type",
            "Location_Description",
            "Beat",
            "District",
            "Latitude",
            "Longitude",
            "Hour",
            "DayOfWeek",
            "prediction",
            F.col("probability").getItem(1).alias("prob_arrest")
        )
    ).alias("value")
)

################################################################################
# Sink
################################################################################

if args.console:
    query = (out_df
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
else:
    query = (out_df
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", args.brokers)
             .option("topic", args.output_topic)
             .option("checkpointLocation", args.checkpoint)
             .start())

query.awaitTermination()
