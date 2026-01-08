from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import signal
import sys
from script.shopee_data import stream_shopee_data
from script.visualize_data import grouping, write_file
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json

# Kafka configuration
kafka_bootstrap_servers = "localhost:9094"
# Comma-separated list of Kafka topics
kafka_topics = "shopee_info,lazada_info"

# Define the schema for the Kafka message
schema = StructType([
    StructField("attrs", StringType(), True),
    StructField("avg_rating", StringType(), True),
    StructField("num_review", StringType(), True),
    StructField("num_sold", StringType(), True),
    StructField("price", StringType(), True),
    StructField("product_desc", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("shipping", StringType(), True),
    StructField("shop_info", StringType(), True),
    StructField("url", StringType(), True)
])

# Your custom function to convert the DataFrame


def stream_shopee_data_first_level(df):
    # Parse JSON strings in the 'value' column according to the schema
    df_converted = df.withColumn("value", from_json(
        col("value"), schema)).select("value.*")
    return df_converted


# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Function to process the data


def process_data(df, epoch_id):
    # Convert the DataFrame using your custom function
    # df_converted = stream_shopee_data(df)
    df_converted = stream_shopee_data_first_level(df)
    df_converted = stream_shopee_data(df_converted)
    df_converted = grouping(df_converted)
    write_file(df_converted)
    # Show the converted DataFrame
    print(f"Batch {epoch_id} received")
    # For demonstration, just show the DataFrame
    df_converted.show(truncate=False)

# Signal handler for graceful shutdown


def signal_handler(sig, frame):
    print('You pressed Ctrl+C! Exiting gracefully...')
    spark.stop()
    sys.exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topics) \
    .load()

# Convert the binary values to strings
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Process the data
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_data) \
    .start()

# Await termination
query.awaitTermination()
