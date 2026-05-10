from pyspark.sql import Window
import redis
import json
import pyspark.sql.functions as F
import os
import sys
from pyspark.sql.functions import col, expr
from setup import get_spark, get_latest_schema
from pyspark.sql.avro.functions import from_avro
from dotenv import load_dotenv

load_dotenv()
# environment set-up
os.environ['HADOOP_HOME'] = os.getenv("HADOOP_PATH")
sys.path.append(os.getenv("HADOOP_PATH")+"/bin")

spark = get_spark()

def dual_write_sink(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    window_spec = Window.partitionBy("ticker").orderBy("candle_ts").rowsBetween(-4, 0)
    
    final_features = batch_df \
        .withColumn("sma_5", F.avg("close").over(window_spec)) \
        .withColumn("volatility", F.col("high") - F.col("low")) \
        .withColumn("signal", F.when(F.col("close") > F.avg("close").over(window_spec), "BUY").otherwise("SELL"))

    final_features.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.analytics_zone.enriched_signals")

    r = redis.Redis(host='localhost', port=6379, db=0)
    latest_rows = final_features.sort(F.col("candle_ts").desc()).dropDuplicates(["ticker"]).collect()
    
    for row in latest_rows:
        payload = {
            "ticker": row['ticker'],
            "price": row['close'],
            "sma_5": round(row['sma_5'], 2) if row['sma_5'] else 0,
            "signal": row['signal'],
            "headline": row['headline'] or "No News",
            "ts": str(row['candle_ts'])
        }
        r.set(f"live:{row['ticker']}", json.dumps(payload))
    
    print(f"✅ Batch {batch_id} processed with Features.")
 
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics_zone")

candles_df = spark.readStream \
    .format("iceberg") \
    .load("local.cleaned_zone.market_candles") \
    .withColumnRenamed("window_start", "candle_ts") \
    .withWatermark("candle_ts", "5 minutes")

sentiment_schema = get_latest_schema("market-sentiment-value")
sentiment_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-sentiment-raw") \
    .load() \
    .select(
        from_avro(expr("substring(value, 6)"), sentiment_schema).alias("news")
    ).select(
        col("news.ticker").alias("news_ticker"),
        col("news.source"),
        col("news.sentiment_score"),
        col("news.headline"),
        col("news.created_at").alias("news_ts")
    ).withWatermark("news_ts", "5 minutes")

enriched_df = candles_df.join(
    sentiment_df,
    expr("""
        ticker = news_ticker AND
        news_ts >= candle_ts - interval 30 minutes AND 
        news_ts <= candle_ts + interval 30 minutes
    """),
    "left"
)
query = enriched_df.writeStream \
    .foreachBatch(dual_write_sink) \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/enriched_signals_v3_fresh") \
    .start()

print("🔗 PiT Joiner is active. Linking headlines to price action...")
query.awaitTermination()