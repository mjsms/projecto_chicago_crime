"""Dashboard rápido (Streamlit) para ver previsões de crimes em tempo real.

Usa kafka-python em thread separada para subscrever o tópico de previsões e
mostra‑as em tabela/contagem.
"""

import streamlit as st
import threading, queue, json
from kafka import KafkaConsumer

KAFKA_BROKERS = st.secrets.get("KAFKA_BROKERS", "localhost:9092")
PRED_TOPIC    = st.secrets.get("KAFKA_TOPIC_OUT", "crime_predictions")

q = queue.Queue()

def consume():
    consumer = KafkaConsumer(
        PRED_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode()),
        group_id="streamlit_dash")
    for msg in consumer:
        q.put(msg.value)

if "thread_started" not in st.session_state:
    threading.Thread(target=consume, daemon=True).start()
    st.session_state["thread_started"] = True

st.title("📈 Crime Predictions – Real‑time")
pla = st.empty()

data = []
while True:
    try:
        item = q.get_nowait()
        data.append(item)
    except queue.Empty:
        break
if data:
    df = st.session_state.get("df")
    import pandas as pd
    if df is None:
        df = pd.DataFrame(data)
    else:
        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    st.session_state["df"] = df
    pla.dataframe(df.tail(50))
