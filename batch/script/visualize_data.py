from pyspark.sql.functions import col, row_number, monotonically_increasing_id, lit, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Window, SparkSession
import argparse

# Initialize SparkSession (không cần Elasticsearch connector)
spark = (SparkSession
         .builder
         .appName("visualize_data")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

def load_file(path):
    schema = StructType([
        StructField("attrs", StringType(), True),
        StructField("avg_rating", DoubleType(), True),
        StructField("num_review", IntegerType(), True),
        StructField("num_sold", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("shipping", StringType(), True),
        StructField("url", StringType(), True),
        StructField("country", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("stock", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("first_category", StringType(), True),
        StructField("second_category", StringType(), True),
        StructField("third_category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("shop_name", StringType(), True),
        StructField("shop_like_tier", IntegerType(), True),
        StructField("shop_num_review", IntegerType(), True),
        StructField("shop_reply_percentage", DoubleType(), True),
        StructField("shop_reply_time", StringType(), True),
        StructField("shop_creation_time", IntegerType(), True),
        StructField("shop_num_follower", IntegerType(), True)
    ])
    df = spark.read.format("csv").option("header", "true").schema(schema).load(path)
    return df


def grouping(df):
    # Logic lọc sản phẩm trùng tên, lấy giá cao nhất
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row", row_number().over(w)) \
           .filter(col("row") == 1) \
           .drop("row")
    return df


def write_file(df, destination):
    # Ghi lại xuống HDFS file clean/visualize
    print(f"Writing CSV to HDFS: {destination}")
    (df
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))
    return df


def get_visualize_data(origin, destination):
    print(f"Reading from: {origin}")
    df = load_file(origin)
    
    df = grouping(df)
    
    # Chỉ ghi HDFS
    write_file(df, destination)
    
    print("Succeed!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get visualize data')
    parser.add_argument('--origin', type=str, required=True, help='Read location')
    parser.add_argument('--destination', type=str, required=True, help='Save location')
    
    args = parser.parse_args()
    get_visualize_data(args.origin, args.destination)
    
    spark.stop()