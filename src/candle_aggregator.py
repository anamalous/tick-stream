import os
import sys
from pyspark.sql.functions import col, expr, window, first, last, min, max, sum
from pyspark.sql.avro.functions import from_avro
from setup import get_latest_schema, get_spark

# environment set-up
os.environ['HADOOP_HOME'] = "E:/hadoop"
sys.path.append("E:/hadoop/bin")

trade_schema_json = get_latest_schema("market-trades-raw-value")
spark = get_spark()


spark.sql("CREATE NAMESPACE IF NOT EXISTS local.cleaned_zone")

# table creation
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.cleaned_zone.market_candles (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        ticker STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume LONG
    )
    USING iceberg
    PARTITIONED BY (days(window_start), ticker)
""")


# read from Kafka 
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-trades-raw") \
    .option("startingOffsets", "latest") \
    .load()

# decode & alias
decoded_df = raw_stream.select(
    from_avro(expr("substring(value, 6)"), trade_schema_json).alias("data")
).select(
    col("data.timestamp").alias("timestamp"),
    col("data.ticker").alias("ticker"),
    col("data.price").cast("double").alias("price"),
    col("data.size").cast("long").alias("size")
)

# use 1-minute window that updates every 10 seconds
candles_df = decoded_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute", "10 seconds"),
        col("ticker")
    ) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("size").alias("volume")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "ticker", "open", "high", "low", "close", "volume"
    )

# write to the Iceberg table
query = candles_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/market_candles") \
    .toTable("local.cleaned_zone.market_candles")

print("Candle Aggregator is running...")
query.awaitTermination()