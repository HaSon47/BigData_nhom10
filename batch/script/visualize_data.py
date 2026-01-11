from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Window, SparkSession
import argparse

# Initialize SparkSession with Elasticsearch connector
spark = (SparkSession
         .builder
         .appName("visualize_data")
         .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2")
         .config("spark.es.nodes", "http://localhost:9200")
         .config("spark.es.net.http.auth.user", "elastic")
         .config("spark.es.net.http.auth.pass", "123123")
         .config("spark.es.nodes.wan.only", "true")
         .getOrCreate())


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
    df = spark.read.format("csv").schema(schema).load(path)
    return df


def grouping(df):
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row", row_number().over(
        w)).filter(col("row") == 1).drop("row")
    return df


def write_data_to_elasticsearch(df, index_name="final_hachi"):
    from elasticsearch import Elasticsearch

    def get_max_id(index_name):
        es = Elasticsearch(["http://localhost:9200"],
                           http_auth=('elastic', '123123'))
        response = es.search(index=index_name, body={
            "size": 1,
            "sort": [{"id": {"order": "desc"}}],
            "_source": ["id"]
        })

        if response['hits']['hits']:
            max_id = response['hits']['hits'][0]['_source']['id']
        else:
            max_id = 0
        return max_id

    max_id = get_max_id(index_name)

    # Add an 'id' column to the DataFrame, starting from max_id + 1
    df = df.withColumn("id", monotonically_increasing_id() + max_id + 1)

    df.write.format("org.elasticsearch.spark.sql")\
        .option("es.nodes", "http://localhost:9200")\
        .option("es.net.http.auth.user", "elastic")\
        .option("es.net.http.auth.pass", "123123")\
        .option("es.nodes.discovery", "false")\
        .option("es.nodes.wan.only", "true")\
        .option("es.index.auto.create", "true")\
        .option("es.mapping.id", "id")\
        .mode("append")\
        .save(index_name)


def write_file(df, destination):
    (df
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))
    write_data_to_elasticsearch(df, "final_hachi")
    print("Succeed!")
    return df


def get_visualize_data(path, destination):
    df = load_file(path)
    df = grouping(df)
    write_file(df, destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get visualize data')
    parser.add_argument('--origin', type=str,
                        required=True, help='Read location')
    parser.add_argument('--destination', type=str,
                        required=True, help='Save location')
    args = parser.parse_args()
    get_visualize_data(args.origin, args.destination)
