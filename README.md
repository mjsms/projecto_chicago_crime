# Sistema de Classificação de Crimes em Chicago

Projeto do Mestrado em Engenharia Informática (ISCTE‑IUL) – Unidade Curricular **Algoritmos para Big Data**

> **Objetivo**  Desenvolver um serviço escalável, batch + stream, para classificar registos de crime da cidade de Chicago em "Violent" / "Non‑Violent" recorrendo a Apache Spark ML e Apache Kafka.

---

## Sumário do Repositório

| Pasta / ficheiro | Descrição |
| ---------------- | --------- |
| `notebooks/` | Jupyter/Databricks notebooks de EDA, pré‑processamento e treino de modelos |
| `pipeline/` | Módulos Python com o **Spark ML Pipeline** final e scripts de execução |
| `streaming/` | Produtor Kafka de dados simulados \+ consumidor/inferência em Spark Structured Streaming |
| `data/` | **Não incluído** – instruções abaixo para descarregar os datasets |
| `docker/` | Compose para cluster single‑node: Spark + Kafka + Zookeeper |
| `report/` | Relatório final em \LaTeX{} (`final_report.tex`, PDF compilado) |
| `README.md` | Este guia |

---

## 1 – Requisitos

* **Docker >= 24** (ou instalação local de Spark 3.5 + Kafka 3)
* Python 3.11 + `pip install -r requirements.txt` (se não usar Docker)
* \~32 GB RAM recomendados para treino completo

---

## 2 – Dados

1. Faça download do *Chicago Crime Dataset* (2001‑2023):
   ```bash
   wget -O data/chicago_crime.csv.gz https://huggingface.co/datasets/gymprathap/Chicago-Crime-Dataset/resolve/main/chicago-crime-dataset.csv.gz
   ```
2. Descomprima e mova para `data/raw/`.
3. Execute o notebook `01_eda_preprocess.ipynb` ou:
   ```bash
   python pipeline/preprocess.py  \
       --input data/raw/chicago-crime-dataset.csv \
       --output data/processed/chicago_ready.parquet
   ```

O script grava dois ficheiros Parquet: **train** (70 %) e **test** (30 %).

---

## 3 – Treino Batch

```bash
python pipeline/train.py \
       --train data/processed/train.parquet \
       --test  data/processed/test.parquet \
       --model-out models/gbt_best
```

* **Algoritmos avaliados:** RandomForest, LogisticRegression, GBT
* Métrica de selecção: *Area Under ROC* (AUC)
* Resultado obtido no relatório: `GBT AUC ≈ 0.8805`

Os tempos de treino (~1 h) referem‑se a um portátil 8 C/32 GB.

---

## 4 – Inferência em Streaming

1. Inicie a stack local:
   ```bash
   docker compose -f docker/stack.yml up -d  # Spark master/worker, ZK, Kafka
   ```
2. Produza eventos a 2 000 msg/s:
   ```bash
   python streaming/kafka_producer.py \
          --source data/processed/test.parquet --topic crimes
   ```
3. Arranque o consumidor Spark:
   ```bash
   spark-submit streaming/stream_inference.py \
       --model-path models/gbt_best --topic crimes
   ```

Latência média medida: **350 ms** (GBT) / **< 200 ms** (LR).

---

## 5 – Resultados Principais

| Modelo | AUC | Tempo de treino |
| ------ | --- | --------------- |
| Logistic Regression | 0.872 | 167 s |
| Random Forest | 0.864 | 2 660 s |
| **Gradient Boosted Trees** | **0.881** | 6 429 s |

*Características mais relevantes*: Latitude, Longitude, Hora, Beat, Distrito.


