"""
This simple code illustrates a Kafka producer:
- read data from a topic in a Kafka messaging system.
- print out the data

As it stands, it should work with any data as long as the data is in JSON 
(one can modify the code to handle other types of data)

We use the Python client library kafka-python from 
    https://kafka-python.readthedocs.io/en/master/.

Also, see https://pypi.org/project/kafka-python/
"""
import argparse, json
from kafka import KafkaConsumer
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, hour, dayofweek
from kafka import KafkaProducer
from pyspark.ml.linalg import DenseVector

spark = SparkSession.builder.appName("CrimeConsumer").getOrCreate()
model = PipelineModel.load("/home/jovyan/code/dados/full_model")

#=======================================================
topic = 'chicago-crime'
# consumer_group = 'my-group'

#=======================================================

# Declare the topic
kafka_consumer = KafkaConsumer(
    bootstrap_servers = 'localhost:9092',
    api_version = (3, 9),
    auto_offset_reset = 'earliest',
    enable_auto_commit = False,
    value_deserializer = lambda x: json.loads(x).encode('utf-8')
    # group_id = consumer_group,
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)
kafka_consumer.subscribe([topic])

print("Starting to listen for messages on topic : " + topic + ". ")

for msg in kafka_consumer:
    raw_event = msg.value
    print("Evento recebido:", raw_event)  

    row = Row(**raw_event)
    df = spark.createDataFrame([row])

    # ✅ APLICA AS TRANSFORMAÇÕES ANTES DE CHAMAR o modelo
    if "Date" not in df.columns:
        print("❌ Evento sem coluna 'Date', ignorado.")
        continue
    df = df.withColumn("Date", to_timestamp("Date", "MM/dd/yyyy HH:mm:ss a")) \
    .withColumn("Hour", hour("Date")) \
    .withColumn("DayOfWeek", dayofweek("Date"))

    prediction = model.transform(df)

    row_pred = prediction.select("prediction", "probability").first()
    row_out = {
    "prediction": float(row_pred["prediction"]),
    "probability": row_pred["probability"].toArray().tolist() if isinstance(row_pred["probability"], DenseVector) else list(row_pred["probability"])
    }
    print("📤 Enviando previsão:", row_out)
    producer.send("crime-predictions", value=row_out)

