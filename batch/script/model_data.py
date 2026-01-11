from pyspark.sql.functions import col, lit, avg, row_number, coalesce, concat_ws, when
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Window
from pyspark.sql import SparkSession
import argparse

spark = (SparkSession
    .builder
    .appName("model_data")
    .getOrCreate())


def load_shopee(path):
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
        StructField("shop_like_tier", StringType(), True),
        StructField("shop_num_review", IntegerType(), True),
        StructField("shop_reply_percentage", DoubleType(), True),
        StructField("shop_reply_time", StringType(), True),
        StructField("shop_creation_time", IntegerType(), True),
        StructField("shop_num_follower", IntegerType(), True)
    ])

    df = spark.read.format("csv").schema(schema).load(path)

    return df

def load_lazada(path):
    schema = StructType([
        StructField("product_name", StringType(), True), 
        StructField("avg_rating", DoubleType(), True), 
        StructField("price", IntegerType(), True), 
        StructField("brand", StringType(), True), 
        StructField("num_review", IntegerType(), True), 
        StructField("attrs", StringType(), True), 
        StructField("category", StringType(), True), 
        StructField("description", StringType(), True), 
        StructField("url", StringType(), True), 
        StructField("first_category", StringType(), True), 
        StructField("second_category", StringType(), True), 
        StructField("third_category", StringType(), True), 
        StructField("shop_name", StringType(), True), 
        StructField("shop_rating", DoubleType(), True), 
        StructField("ship_on_time", DoubleType(), True), 
        StructField("shop_reply_percentage", DoubleType(), True)
    ])

    df = spark.read.format("csv").schema(schema).load(path)

    return df


def fill_with_mean(df):

    columns = ['avg_rating', 
                'shop_rating', 
                'ship_on_time', 
                'shop_reply_percentage', 
                'num_review', 'num_sold', 
                'shop_num_review', 
                'shop_reply_percentage', 
                'shop_creation_time', 
                'shop_num_follower']

    df = df.select(
        *(
            coalesce(col(column), avg(column).over(Window.orderBy(lit(1)))).alias(column)
            if column in columns
            else col(column)
            for column in df.columns
        )
    )
    return df


def drop_null_record(df):
    df = df.filter(df["price"].isNotNull())
    df = df.filter(df["product_name"].isNotNull())
    df = df.filter("product_name != ''")
    df = df.filter(df["shop_name"].isNotNull())
    df = df.filter(df["description"].isNotNull())

    return df


def fill_with_blank(df):
    df = df.na.fill("",["shipping", "country", "origin"])
    return df


def fill_with_no_info(df):
    df = df.withColumn("brand", when(df["brand"] == '', 'no info').otherwise(df["brand"]))
    df = df.na.fill("no info", ['first_category', 'second_category', 'third_category', 'shop_like_tier', 'shop_reply_time'])
    return df


def concat_columns(df):
    df =  df.withColumn('name_description', concat_ws(' ', 'product_name', 'description'))
    df =  df.withColumn('augmented_description', concat_ws(' ', 'product_name', 'description', 'shipping', 'country'))
    return df


def drop_redundant_columns(df):
    df = df.drop(col("url"))
    df = df.drop(col("attrs"))
    df = df.drop(col("shipping"))
    df = df.drop(col("country"))
    df = df.drop(col("stock"))
    return df

def grouping(df):
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")
    return df

def write_file(df, destination):

    (df  
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))

    print("Succeed!")
    return df

def get_model_data(shopee_path, lazada_path, destination):

    # Load data
    shopee_df = load_shopee(shopee_path)
    lazada_df = load_lazada(lazada_path)
    df = shopee_df.unionByName(lazada_df, allowMissingColumns=True)

    df = fill_with_mean(df)
    df = drop_null_record(df)
    df = fill_with_blank(df)
    df = fill_with_no_info(df)
    df = concat_columns(df)
    df = drop_redundant_columns(df)

    df = grouping(df)
    write_file(df, destination)
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get model data')

    parser.add_argument('--shopee', 
                        type=str,
                        help='Shopee read location')

    parser.add_argument('--lazada', 
                        type=str,
                        help='Lazada read location')

    parser.add_argument('--destination', 
                        type=str,
                        help='Save location')

    args = parser.parse_args()

    get_model_data(args.shopee, args.lazada, args.destination)