"""Envia registos de crimes (CSV/Parquet) para um tópico Kafka como JSON.

Uso:
    python producer_crime_kafka.py --csv ../dados/chicago_crime_small.parquet --topic chicago_crimes
"""

import argparse, json, time, sys, pathlib
import pandas as pd
from kafka import KafkaProducer

def read_data(path: str, limit: int | None = None) -> pd.DataFrame:
    path = pathlib.Path(path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    if limit:
        df = df.head(limit)
    return df[[
        "Date", "Primary_Type", "Location_Description",
        "Beat", "District", "Latitude", "Longitude"
    ]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV ou Parquet de origem")
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--topic", default="crime")
    ap.add_argument("--sleep", type=float, default=0.05,
                    help="Delay entre mensagens (s)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Limita nº de linhas (debug)")
    args = ap.parse_args()

    df = read_data(args.csv, args.limit)
    prod = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    try:
        for _, row in df.iterrows():
            prod.send(args.topic, row.to_dict())
            print("→", row.to_dict())
            time.sleep(args.sleep)
        prod.flush()
    finally:
        prod.close()

if __name__ == "__main__":
    main()
