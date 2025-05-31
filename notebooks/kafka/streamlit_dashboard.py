import streamlit as st
import threading, queue, json, time
from kafka import KafkaConsumer
import pandas as pd

# 🔁 Queue global — fora de qualquer função!
q = queue.Queue()

# 🔧 Configurações
KAFKA_BROKERS = "localhost:9092"
PRED_TOPIC = "crime_predictions"

# 🔄 Thread que consome do Kafka e mete na queue global
def consume():
    consumer = KafkaConsumer(
        PRED_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode()),
        group_id="streamlit_dash"
    )
    for msg in consumer:
        print("📥 RECEBIDO NO STREAMLIT:", msg.value)
        q.put(msg.value)

# 🧠 Inicia thread só uma vez
if "thread_started" not in st.session_state:
    threading.Thread(target=consume, daemon=True).start()
    st.session_state["thread_started"] = True

# 🗂️ Inicia DataFrame se necessário
if "df" not in st.session_state:
    st.session_state["df"] = pd.DataFrame()

# 🖼️ Layout
st.set_page_config(page_title="Crime Predictions", layout="wide")
st.title("📈 Crime Predictions – Real‑time")

# 🧮 Sidebar com métrica
with st.sidebar:
    st.header("📊 Dados em tempo real")
    st.metric("Previsões recebidas", len(st.session_state["df"]))

# 📨 Puxa da queue
new_data = []
while not q.empty():
    new_data.append(q.get())

# ➕ Junta novos dados ao DataFrame
if new_data:
    df_new = pd.DataFrame(new_data)
    st.session_state["df"] = pd.concat([st.session_state["df"], df_new], ignore_index=True)

# 📊 Mostra últimos dados
if not st.session_state["df"].empty:
    st.dataframe(st.session_state["df"].tail(50), use_container_width=True)
else:
    st.info("⏳ Aguardando previsões do modelo...")

# ⏲️ Atualiza a cada 5s
st.markdown("⏳ Atualizando automaticamente a cada 5 segundos...")
time.sleep(5)
st.rerun()
