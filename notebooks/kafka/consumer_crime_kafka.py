"""Consumer simples para ler eventos de crimes de um tópico Kafka."""

import argparse, json
from kafka import KafkaConsumer

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--topic", required=True)
    p.add_argument("--group", default="crime_cli")
    args = p.parse_args()

    cons = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group,
        value_deserializer=lambda v: json.loads(v.decode())
    )
    print("⏳ A consumir… Ctrl‑C para parar.")
    try:
        for msg in cons:
            print(f"[{msg.offset}] {msg.value}")
    except KeyboardInterrupt:
        pass
    finally:
        cons.close()

if __name__ == "__main__":
    main()
