import requests
from pyspark.sql import SparkSession


def get_latest_schema(subject): # fetch latest schema from Registry (for decoding)
    res = requests.get(f"http://localhost:8081/subjects/{subject}/versions/latest")
    return res.json()['schema']

_spark = None
def get_spark(): # initialize Spark with Iceberg and AWS configurations
    global _spark
    if not _spark:
        _spark = SparkSession.builder \
            .appName("AlphaEngine-Ingestion") \
            .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"  # Matches Spark 3.5
                "org.apache.spark:spark-avro_2.12:3.5.3,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0," 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    return _spark
        
