from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import requests

# 1. Fetch Schema from Registry
schema_json = requests.get("http://localhost:8081/subjects/market-trades-raw-value/versions/latest").json()['schema']

spark = SparkSession.builder \
    .appName("AlphaEngine-Ingest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# 2. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-trades-raw") \
    .load()

# 3. Decode Avro (Skip 5-byte header)
decoded_df = raw_df.select(
    from_avro(expr("substring(value, 6)"), schema_json).alias("data")
).select("data.*")

# 4. Sink to Iceberg
query = decoded_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "local.raw.market_trades") \
    .option("checkpointLocation", "/tmp/checkpoints/trades") \
    .start()

query.awaitTermination()