"""Consumer simples para ler eventos de crimes de um tópico Kafka."""

import argparse, json
from kafka import KafkaConsumer
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, hour, dayofweek
from kafka import KafkaProducer
from pyspark.ml.linalg import DenseVector

spark = SparkSession.builder.appName("CrimeConsumer").getOrCreate()
model = PipelineModel.load("/home/jovyan/code/dados/full_model")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="crime_cli")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda v: json.dumps(v).encode()
    )

    cons = KafkaConsumer(
        "crime",
        bootstrap_servers=args.brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group,
        value_deserializer=lambda v: json.loads(v.decode())
    )
    print("⏳ A consumir… Ctrl‑C para parar.")
    try:
        for msg in cons:
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
            producer.send("crime_predictions", value=row_out)


    except KeyboardInterrupt:
        pass
    finally:
        cons.close()

if __name__ == "__main__":
    main()


