from pyspark.sql.functions import lower, regexp_replace, regexp_extract, col, trim, when, instr, lit, concat_ws, size, split, avg, isnan, when, count, isnull, mean, coalesce
from pyspark.sql.types import StructType,StructField, StringType, MapType
from pyspark.sql import SparkSession
import argparse

special_char = '[^a-z0-9A-Z_ ' \
               'àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬ' \
               'ÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]+'

spark = (SparkSession
    .builder
    .appName("full_lazada_data")
    .getOrCreate())

def load_file(path):

    # to convert attrs to String
    schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("avg_rating", StringType(), True),
        StructField("price", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("num_review", StringType(), True),
        StructField("attrs", MapType(StringType(),StringType(),True)), 
        StructField("category", StringType(), True),
        StructField("shop_info", StringType(), True),
        StructField("product_desc", StringType(), True),
        StructField("url", StringType(), True)
    ])
    df = spark.read.format("json").schema(schema).load(path)

    return df

def clean_product_name(df):
    # Lowercase
    product_name = lower(col('product_name'))

    # # Remove like tier
    # product_name = regexp_replace(product_name, 'yêu thích\n|yêu thích\+\n', ' ')

    # Remove contents inside [], option indicate promotion, prices    
    product_name = regexp_replace(product_name, r'\[.*?\]', ' ')

    # Remove contents inside [], option indicate promotion, prices    
    product_name = regexp_replace(product_name, r'\(.*?\)', ' ')

    # Remove special character
    product_name = regexp_replace(product_name, special_char, ' ')
    # Remove redundant whitespaces  
    product_name = regexp_replace(product_name, ' +', ' ')

    # Trim
    product_name = trim(product_name)
    return df.withColumn('product_name', product_name)

def clean_price(df):
    value = col('price')   
    value = regexp_replace(value, '₫' , '')
    value = regexp_replace(value, ',', '')
    value = value.cast('int')

    return df.withColumn('price', value)

def clean_brand(df):
    brand_name = lower(col('brand_name'))
    brand_name = regexp_replace(brand_name, 'no brand', 'no info')
    brand_name = regexp_replace(brand_name, special_char, ' ')
    brand_name = regexp_replace(brand_name, ' +', ' ')
    brand_name = trim(brand_name)
    return df.withColumn('brand_name', brand_name)

def clean_review(df):
    num_review = lower(col('num_review'))
    num_review = regexp_replace(num_review, 'no ratings', '0')
    num_review = regexp_replace(num_review, ' ratings', '')
    # num_review = regexp_replace(num_review, 'không có đánh giá', '0')
    # num_review = regexp_replace(num_review, ' đánh giá', '')
    num_review = num_review.cast('int')

    return df.withColumn('num_review', num_review)

def clean_attrs(df):
    attrs = lower(col('attrs').cast('string'))
    attrs = regexp_replace(attrs, special_char, ' ')
    attrs = regexp_replace(attrs, ' +', ' ')
    attrs = trim(attrs)
    return df.withColumn('attrs', attrs)

def clean_desc(df):
    product_desc = lower(col('product_desc'))
    product_desc = regexp_replace(product_desc, ' &amp;', ',')
    product_desc = regexp_replace(product_desc, '<svg.*?</svg>|<div>|div|class=|"|<label.*?>|<flex.*?>| href=/', '')
    product_desc = regexp_replace(product_desc, '</a>', '-')
    product_desc = regexp_replace(product_desc, '</label>', ': ')
    product_desc = regexp_replace(product_desc, '< ', '<')
    product_desc = regexp_replace(product_desc, "\/.*?\>","/>")
    product_desc = regexp_replace(product_desc, '<a ', '<')
    product_desc = regexp_replace(product_desc, '<p ', '<')

    product_desc = regexp_replace(product_desc, '</>', ' ')
    product_desc = regexp_replace(product_desc, '<.*?>', ' ')
    product_desc = regexp_replace(product_desc, '\\n', ' ')
    product_desc = regexp_replace(product_desc, special_char, ' ')

    product_desc = regexp_replace(product_desc, ' +', ' ')
    product_desc = trim(product_desc)

    return df.withColumn('product_desc', product_desc)

def extract_first_category(df):
    category = regexp_extract('category', '(.+?)/', 1)
    return df.withColumn('first_category', category)

def extract_second_category(df):
    category = col('category')
    cat_list = split(category, r"/")

    return df.withColumn('second_category', 
        when (
            size(cat_list) > 1,
            concat_ws(' / ',cat_list[0],cat_list[1])
        ).otherwise('no info')
    )

def extract_third_category(df):
    category = col('category')
    cat_list = split(category, r"/")

    return df.withColumn('third_category', 
        when (
            size(cat_list) > 2,
            concat_ws(' / ',cat_list[0],cat_list[1], cat_list[2])
        ).otherwise('no info')
    )

def extract_shop_name(df):
    shop_info = col('shop_info')
    shop_name = regexp_extract(shop_info, '\\n(.+?)\\n', 1)
    return df.withColumn('shop_name', shop_name)

def extract_shop_rating(df):
    shop_info = col('shop_info')
    shop_rating = regexp_extract(shop_info, 'Seller Ratings\\n(.+?)\\n', 1)
    shop_rating = regexp_replace(shop_rating, '%', '')
    shop_rating = shop_rating.cast('float') / 100
    return df.withColumn('shop_rating', shop_rating)

def extract_ship_on_time(df):
    shop_info = col('shop_info')
    ship_on_time = regexp_extract(shop_info, 'Ship On Time\\n(.+?)\\n', 1)
    ship_on_time = regexp_replace(ship_on_time, '%', '')
    ship_on_time = ship_on_time.cast('float') / 100
    return df.withColumn('ship_on_time', ship_on_time)

def extract_shop_reply_percentage(df):
    shop_info = col('shop_info')
    shop_reply_percentage = regexp_extract(shop_info, 'Chat Response\\n(.+?)\\n', 1)
    shop_reply_percentage = regexp_replace(shop_reply_percentage, '%', '')
    shop_reply_percentage = shop_reply_percentage.cast('float') / 100
    return df.withColumn('shop_reply_percentage', shop_reply_percentage)

def write_file(df, destination):

    (df  
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))

    return df

def get_full_data(origin, destination):

    # Load
    df = load_file(origin)

    # Clean
    df = clean_product_name(df)
    df = clean_price(df)
    df = clean_brand(df)
    df = clean_review(df)
    df = clean_attrs(df)
    df = clean_desc(df)
    df = extract_first_category(df)
    df = extract_second_category(df)
    df = extract_third_category(df)
    df = extract_shop_name(df)
    df = extract_shop_rating(df)
    df = extract_ship_on_time(df)
    df = extract_shop_reply_percentage(df)

    # Cast
    df = df.withColumn("avg_rating",df["avg_rating"].cast('double'))

    # Rename
    df = df.withColumnRenamed('brand_name', 'brand')
    df = df.withColumnRenamed('product_desc', 'description')

    # Drop
    df = df.drop("shop_info")
    df = df.drop("category")

    # Write
    write_file(df, destination)

    print("Succeed!")
    return df



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get full lazada data')

    parser.add_argument('--origin', 
                        type=str,
                        help='Read location')

    parser.add_argument('--destination', 
                        type=str,
                        help='Save location')

    args = parser.parse_args()

    get_full_data(args.origin, args.destination)