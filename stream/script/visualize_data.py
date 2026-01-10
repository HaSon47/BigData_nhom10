from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Window, SparkSession
import argparse
from elasticsearch import Elasticsearch

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


def create_index_with_mapping(index_name):
    es = Elasticsearch(["http://localhost:9200"],
                       http_auth=('elastic', '123123'))
    # Check if the index already exists
    if not es.indices.exists(index=index_name):
        # Define the mapping
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "long"},
                    "attrs": {"type": "text"},
                    "avg_rating": {"type": "double"},
                    "num_review": {"type": "integer"},
                    "num_sold": {"type": "integer"},
                    "price": {"type": "integer"},
                    "product_name": {"type": "text"},
                    "shipping": {"type": "text"},
                    "url": {"type": "text"},
                    "country": {"type": "text"},
                    "brand": {"type": "text"},
                    "stock": {"type": "text"},
                    "origin": {"type": "text"},
                    "first_category": {"type": "text"},
                    "second_category": {"type": "text"},
                    "third_category": {"type": "text"},
                    "description": {"type": "text"},
                    "shop_name": {"type": "text"},
                    "shop_like_tier": {"type": "integer"},
                    "shop_num_review": {"type": "integer"},
                    "shop_reply_percentage": {"type": "double"},
                    "shop_reply_time": {"type": "text"},
                    "shop_creation_time": {"type": "integer"},
                    "shop_num_follower": {"type": "integer"}
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        print(f"Created index {index_name} with mapping.")


def grouping(df):
    '''Nếu các product cùng product_name, lấy sản phẩm có giá cao nhất'''
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row", row_number().over(
        w)).filter(col("row") == 1).drop("row")
    return df


def check_data_existed_in_elk(df):
    pass


def write_data_to_elasticsearch(df, index_name="final_hachi"):
    # create_index_with_mapping(index_name)
    check_data_existed_in_elk(df)

    def get_max_id(index_name):
        es = Elasticsearch(["http://localhost:9200"],
                           http_auth=('elastic', '123123'))
        
        if not es.indices.exists(index=index_name):
            return 0
        
        try:
            response = es.search(index=index_name, body={
                "size": 1,
                "sort": [{
                    "id": {
                        "order": "desc",
                        "unmapped_type": "long"  # <--- CRITICAL FIX
                    }
                }],
                "_source": ["id"]
            })

            if response['hits']['hits']:
                max_id = response['hits']['hits'][0]['_source']['id']
            else:
                max_id = 0
            return max_id
        except Exception as e:
            print(f"Warning: Could not retrieve max_id, defaulting to 0. Error: {e}")
            return 0

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


# def write_file(df, destination):
#     (df
#         .coalesce(1)
#         .write.option("header", True)
#         .format("csv")
#         .mode('overwrite')
#         .csv(destination))
#     write_data_to_elasticsearch(df, "final_hachi")
#     print("Succeed!")
#     return df

def write_file(df):
    write_data_to_elasticsearch(df, "final_hachi")
    print("Succeed!")
    return df

