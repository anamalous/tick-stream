# 🛰️ Tick-Stream: Market Intelligence Pipeline

Tick-Stream is a high-frequency trading and sentiment analysis platform. It leverages a modern data stack to ingest live market ticks and global news intel, processing them through a distributed Spark pipeline to generate real-time "Alpha" signals.

## 🏗️ System Architecture

The data flows through a **Kappa Architecture** designed for sub-second latency:

1. **Ingestion Layer:** Real-time producers (`producer_live`, `macro_producer`, `news_producer`) fetch live data from AlphaVantage and NewsAPI, serializing them via **Avro** and pushing them into **Kafka** topics.
2. **Processing Layer (The Engine):** 
* **Spark Structured Streaming** consumes raw Kafka feeds.
* **Sentiment Engine** runs NLP analysis on news headlines.
* **PIT Joiner** performs "Point-In-Time" joins to align sentiment pulses with price ticks.

3. **Storage & Speed Layer:** 
* **Apache Iceberg (on MinIO S3):** Acts as the historical "Source of Truth" for model training.
* **Redis:** Serves as the "Speed Layer," holding the latest snapshots for the frontend.


4. **Presentation Layer:** A **FastAPI** backend serves the Redis snapshots via WebSockets/REST to a **React** dashboard powered by `lightweight-charts`.

---

## 🛠️ Environment Setup

### 1. Windows Pre-requisites (Hadoop/Spark Fix)

Because Spark relies on HDFS-style file permissions, you must mock the Hadoop environment:

* Create a folder (e.g., `C:\hadoop`).
* Download and place `winutils.exe` and `hadoop.dll` inside `C:\hadoop\bin`.
* Create a folder at `E:\spark_tmp` to act as the shuffle/local buffer.

### 2. Configuration (`.env`)

Create a `.env` file in the root directory:

```env
HADOOP_HOME="C:\hadoop"
SPARK_TEMP_PATH="E:\spark_tmp"
ALPHA_KEY="your_alpha_vantage_key"
NEWS_KEY="your_newsapi_org_key"
```

### 3. Python & Dependencies

* **Version:** Python 3.11.x
* **Setup:**
```bash
python -m venv trade_env
source trade_env/Scripts/activate
pip install -r requirements.txt
```



---

## 🐳 Infrastructure (Docker)

1. **Launch Containers:** 
```
> bash
docker-compose up -d
```
*This starts Kafka, Zookeeper, Schema Registry, Redis, and MinIO.*



2. **Initialize MinIO:** 
* Access `http://localhost:9001`.
* Create a bucket named `warehouse`.


3. **Register Schemas:** Run the schema registration script to upload `schemas/trade.avsc` and `schemas/sentiment.avsc` to the Schema Registry.
```
# for trades schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(jq -Rs . < schemas/trade.avsc)}"\
  http://localhost:8081/subjects/market-trade-raw-value/versions

# for sentiment schema
  curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(jq -Rs . < schemas/sentiment.avsc)}"\
  http://localhost:8081/subjects/market-sentiment-value/versions

```

---

## 🚀 Execution Sequence

To start the terminal, run the components in this specific order:

### Phase 1: The Feed (Producers)

Open four terminals and run:

1. `python src/producers/producer_live.py` (Market Ticks)
2. `python src/producers/sentiment_producer_live.py` (News Stream)
3. `python src/producers/macro_producer.py` (Vitals/CPI/Inflation)
4. `python src/producers/sentiment_engine.py` (NLP Scoring)

### Phase 2: The Brain (Spark Jobs)

1. `python src/spark/spark_ingest.py` (Raw Ingestion to Iceberg)
2. `python src/spark/candle_aggregator.py` (OHLCV Generation)
3. `python src/spark/pit_joiner.py` (The Feature Matrix Join)

### Phase 3: The API & UI

1. **Backend:** `python src/backend/main.py` (FastAPI)
2. **Frontend:** 
```
> bash
cd alpha-dashboard
npm install
npm run dev
```



---

## 🧹 Maintenance (Clearing Stale Data)

If you need to reset the system for a clean run:

* **Clear Kafka:** Delete and recreate the `market-news-raw` and `market-trades-raw` topics.
* **Clear Spark:** Manually delete the `E:\spark_tmp` and your `checkpoint/` directories.
* **Clear Redis:** Run `FLUSHALL` via `redis-cli`.
