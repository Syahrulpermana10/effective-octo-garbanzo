import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

checkpoint_location = "/temp/checkpoint_location"
os.makedirs(checkpoint_location, exist_ok=True)

conf = (
    pyspark.SparkConf()
    .setAppName("DibimbingStreaming")
    .setMaster(spark_host)
    .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
)
sparkcontext = pyspark.SparkContext.getOrCreate(conf=conf)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

json_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("furniture", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("ts", LongType(), True)
])

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING)").select(
    from_json("value", json_schema).alias("data")
).select("data.ts", "data.price")

parsed_stream_df = parsed_stream_df.withColumn("timestamp", from_unixtime("ts").cast(TimestampType()))

windowed_stream_df = parsed_stream_df.withWatermark("timestamp", "1 day").groupBy(
    window("timestamp", "1 day")
).agg(count("*").alias("data_count"), sum("price").alias("total_price"))

query = (
    windowed_stream_df.writeStream.format("console")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", checkpoint_location)
    .start()
)

query.awaitTermination()