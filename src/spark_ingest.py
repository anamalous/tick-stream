import os
import sys
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.avro.functions import from_avro
from gx_context import get_validation_def
from setup import get_latest_schema, get_spark

os.environ['HADOOP_HOME'] = "E:/hadoop"
sys.path.append("E:/hadoop/bin")

trade_schema_json = get_latest_schema("market-trades-raw-value")
spark = get_spark()

# create the database/namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.raw_zone")

# spark.sql("DROP TABLE IF EXISTS local.raw_zone.market_trades")
# spark.sql("DROP TABLE IF EXISTS local.raw_zone.dead_letter_trades")

# create table definition
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.raw_zone.market_trades (
        timestamp TIMESTAMP,
        ticker STRING,
        price DOUBLE,
        size LONG,
        exchange STRING,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(timestamp))
""")

# create DLO table 
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.raw_zone.dead_letter_trades (
        timestamp TIMESTAMP,
        ticker STRING,
        price DOUBLE,
        size LONG,
        exchange STRING,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(timestamp))
""")

def validate_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Spark filters
    clean_df = batch_df.filter((col("price") > 0) & (col("size") > 0) & (col("ticker").isNotNull()))
    
    # Great Expectations Audit
    results = get_validation_def().run(batch_parameters={"dataframe": clean_df})
    
    if results.success:
        clean_df.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.raw_zone.market_trades")
    else:
        print("Diverting to DLO.")
        clean_df.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.raw_zone.dead_letter_trades")

# Read Stream from Kafka
trades_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-trades-raw") \
    .option("startingOffsets", "latest") \
    .load()

# Decode Avro (Skipping 5-byte Confluent Header)
mapped_df = trades_raw_df.select(
    from_avro(expr("substring(value, 6)"), trade_schema_json).alias("data")
).select(
    col("data.timestamp").alias("timestamp"),
    col("data.ticker").alias("ticker"),
    col("data.price").cast("double").alias("price"),
    col("data.size").cast("long").alias("size"),
    col("data.exchange").alias("exchange")
)
# Track when data hit our system
final_df = mapped_df.withColumn("processed_at", current_timestamp())

# Write to Iceberg
query = final_df.writeStream \
    .foreachBatch(validate_and_write) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/market_trades") \
    .start()

print("Ingestion Engine is running...")
query.awaitTermination()