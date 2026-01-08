from pyspark.sql.functions import lower, regexp_replace, regexp_extract, col, trim, when, instr, lit, concat_ws, size, split, avg, isnan, when, count, isnull, mean, coalesce
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
import argparse
special_char = '[^a-z0-9A-Z_ ' \
               'àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬ' \
               'ÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]+'

spark = (SparkSession
         .builder
         .appName("full_shopee_data")
         .getOrCreate())


def load_file(path):

    # to convert attrs to String
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
    df = spark.read.format("json").schema(schema)\
        .load(path)

    return df


def k_to_number(c):  # 3,2k -> 3200
    contain_comma = instr(c, ',') >= 1
    c = when(contain_comma, regexp_replace(c, 'k', '00')) \
        .otherwise(regexp_replace(c, 'k', '000'))
    c = regexp_replace(c, ',', '')
    return c


def clean_product_name(df):
    # Lowercase
    product_name = lower(col('product_name'))
    # Remove like tier
    product_name = regexp_replace(
        product_name, 'yêu thích\n|yêu thích\+\n', ' ')
    # Remove contents inside [], option indicate promotion, prices
    product_name = regexp_replace(product_name, r'\[.*?\]', ' ')
    # Remove special character
    product_name = regexp_replace(product_name, special_char, ' ')
    # Remove redundant whitespaces
    product_name = regexp_replace(product_name, ' +', ' ')

    # Trim
    product_name = trim(product_name)
    return df.withColumn('product_name', product_name)


def clean_price(df):
    value = col('price')
    value = regexp_replace(value, r'.*₫', '')
    value = regexp_replace(value, '\.', '')
    return df.withColumn('price', value)


def clean_desc(df):
    product_desc = lower(col('product_desc'))
    product_desc = regexp_replace(product_desc, 'Vớ/ Tất', 'Vớ, Tất')
    product_desc = regexp_replace(product_desc, 'Vớ/Tất', 'Vớ, Tất')
    product_desc = regexp_replace(
        product_desc, 'Quần Dài/Quần Âu', 'Quần Dài, Quần Âu')
    product_desc = regexp_replace(
        product_desc, 'Quần Dài/ Quần Âu', 'Quần Dài, Quần Âu')
    product_desc = regexp_replace(product_desc, ' &amp;', ',')
    product_desc = regexp_replace(
        product_desc, '<svg.*?</svg>|<div>|div|class=|"|<label.*?>|<flex.*?>| href=/', '')
    product_desc = regexp_replace(product_desc, '</a>', '-')
    product_desc = regexp_replace(product_desc, '</label>', ': ')
    product_desc = regexp_replace(product_desc, '< ', '<')
    product_desc = regexp_replace(product_desc, "\/.*?\>", "/>")
    product_desc = regexp_replace(product_desc, '<a ', '<')
    product_desc = regexp_replace(product_desc, '<p ', '<')
    product_desc = regexp_replace(product_desc, ' +', ' ')
    product_desc = trim(product_desc)

    # Split
    product_desc = regexp_replace(product_desc, '</>', '/')
    product_desc = regexp_replace(product_desc, '<.*?>', '')

    return df.withColumn('product_desc', product_desc)


def extract_country(df):
    country = regexp_replace(col('product_desc'),  'mô tả sản phẩm(.*)', '')
    country = regexp_extract(country, 'xuất xứ: (.+?)/', 1)
    country = regexp_replace(country, special_char, ' ')

    return df.withColumn('country', country)


def extract_brand(df):
    brand = regexp_replace(col('product_desc'),  'mô tả sản phẩm(.*)', '')
    brand = regexp_extract(brand, 'thương hiệu: (.+?)-/', 1)
    brand = regexp_replace(brand, special_char, ' ')
    return df.withColumn('brand', brand)


def extract_stock(df):
    stock = regexp_replace(col('product_desc'),  'mô tả sản phẩm(.*)', '')
    stock = regexp_extract(stock, 'kho hàng: (.+?)/', 1)
    stock = regexp_replace(stock, special_char, ' ')
    return df.withColumn('stock', stock)


def extract_origin(df):
    origin = regexp_replace(col('product_desc'),  'mô tả sản phẩm(.*)', '')
    origin = regexp_extract(origin, 'gửi từ: (.+?)/', 1)
    return df.withColumn('origin', origin)


def extract_first_category(df):
    category = regexp_extract('product_desc', 'shopee-(.+?)-', 1)
    return df.withColumn('first_category', category)


def extract_second_category(df):
    category = regexp_extract('product_desc', 'shopee-(.+)-//', 1)
    cat_list = split(category, r"-")

    return df.withColumn('second_category',
                         when(
                             size(cat_list) > 1,
                             concat_ws(' / ', cat_list[0], cat_list[1])
                         ).otherwise('no info')
                         )


def extract_third_category(df):
    category = regexp_extract('product_desc', 'shopee-(.+)-//', 1)
    cat_list = split(category, r"-")

    return df.withColumn('third_category',
                         when(
                             size(cat_list) > 2,
                             concat_ws(
                                 ' / ', cat_list[0], cat_list[1], cat_list[2])
                         ).otherwise('no info')
                         )


def extract_description(df):
    description = col('product_desc')
    description = regexp_replace(description, special_char, ' ')
    description = regexp_extract(description, 'mô tả sản phẩm(.*)', 1)
    description = regexp_replace(description, special_char, ' ')
    description = regexp_replace(description, ' +', ' ')
    description = trim(description)

    return df.withColumn('description', description)


def clean_attrs(df):
    attrs = lower(col('attrs'))
    attrs = regexp_replace(attrs, special_char, ' ')
    attrs = trim(attrs)
    return df.withColumn('attrs', attrs)


def extract_shop_name(df):
    remove_like_tier = regexp_replace(
        col('shop_info'), 'Yêu Thích\n|Yêu Thích\+\n', '')
    shop_name = regexp_extract(remove_like_tier, '(.+?)\n', 1)
    return df.withColumn('shop_name', shop_name)


def extract_shop_like_tier(df):
    shop_like_tier = regexp_extract(col('shop_info'), '^(Yêu Thích\+?)\n', 1)
    shop_like_tier = when(shop_like_tier == "Yêu Thích+", 2) \
        .when(shop_like_tier == "Yêu Thích", 1) \
        .otherwise(0)
    return df.withColumn('shop_like_tier', shop_like_tier)


def extract_shop_num_review(df):
    shop_num_review = regexp_extract(col('shop_info'), 'Đánh Giá\n(.+)\n', 1)
    shop_num_review = k_to_number(shop_num_review)
    shop_num_review = shop_num_review.cast('int')
    return df.withColumn('shop_num_review', shop_num_review)


def extract_shop_reply_percentage(df):
    shop_reply_percentage = regexp_extract(
        col('shop_info'), 'Tỉ Lệ Phản Hồi\n(.+)\n', 1)
    shop_reply_percentage = regexp_replace(shop_reply_percentage, '%', '')
    shop_reply_percentage = shop_reply_percentage.cast('float') / 100
    return df.withColumn('shop_reply_percentage', shop_reply_percentage)


def extract_shop_reply_time(df):
    shop_reply_time = regexp_extract(
        col('shop_info'), 'Thời Gian Phản Hồi\n(.+)\n', 1)
    return df.withColumn('shop_reply_time', shop_reply_time)


def extract_shop_creation_time(df):
    shop_creation_time = regexp_extract(
        col('shop_info'), 'Tham Gia\n(.+)\n', 1)
    num = regexp_extract(shop_creation_time, '\d+', 0).cast('int')
    contain_year = instr(shop_creation_time, 'năm') >= 1
    contain_month = instr(shop_creation_time, 'tháng') >= 1
    shop_creation_time = when(contain_year, num * 12) \
        .otherwise(when(contain_month, num)
                   .otherwise(lit(0)))
    return df.withColumn("shop_creation_time", shop_creation_time)


def extract_shop_num_follower(df):
    shop_num_follower = regexp_extract(
        col('shop_info'), 'Người Theo Dõi\n(.+)', 1)
    shop_num_follower = k_to_number(shop_num_follower)
    shop_num_follower = shop_num_follower.cast('int')
    return df.withColumn("shop_num_follower", shop_num_follower)


def clean_shipping(df):
    shipping = lower(col('shipping'))
    shipping = regexp_replace(shipping, special_char, '')
    shipping = regexp_extract(shipping, r'\d+', 0)
    return df.withColumn('shipping', shipping)


def clean_numeric_field(df, col_name):
    cleaned_field = k_to_number(col(col_name))
    cleaned_field = cleaned_field.cast('int')
    return df.withColumn(col_name, cleaned_field)


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
    df = clean_desc(df)
    df = extract_country(df)
    df = extract_brand(df)
    df = extract_stock(df)
    df = extract_origin(df)
    df = extract_first_category(df)
    df = extract_second_category(df)
    df = extract_third_category(df)
    df = extract_description(df)
    df = clean_attrs(df)
    df = extract_shop_name(df)
    df = extract_shop_like_tier(df)
    df = extract_shop_num_review(df)
    df = extract_shop_reply_percentage(df)
    df = extract_shop_reply_time(df)
    df = extract_shop_creation_time(df)
    df = extract_shop_num_follower(df)
    df = clean_shipping(df)

    df = clean_numeric_field(df, "num_sold")
    df = clean_numeric_field(df, "num_review")

    df = df.drop("product_desc")
    df = df.drop("shop_info")

    df = df.withColumn("shop_like_tier", df["shop_like_tier"].cast('string'))

    # Write
    write_file(df, destination)

    print("Succeed!")
    return df


def stream_shopee_data(df):
    # Clean
    df = clean_product_name(df)
    df = clean_price(df)
    df = clean_desc(df)
    df = extract_country(df)
    df = extract_brand(df)
    df = extract_stock(df)
    df = extract_origin(df)
    df = extract_first_category(df)
    df = extract_second_category(df)
    df = extract_third_category(df)
    df = extract_description(df)
    df = clean_attrs(df)
    df = extract_shop_name(df)
    df = extract_shop_like_tier(df)
    df = extract_shop_num_review(df)
    df = extract_shop_reply_percentage(df)
    df = extract_shop_reply_time(df)
    df = extract_shop_creation_time(df)
    df = extract_shop_num_follower(df)
    df = clean_shipping(df)

    df = clean_numeric_field(df, "num_sold")
    df = clean_numeric_field(df, "num_review")

    df = df.drop("product_desc")
    df = df.drop("shop_info")

    df = df.withColumn("shop_like_tier", df["shop_like_tier"].cast('string'))

    # Write

    print("Succeed!")
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get full shopee data')

    parser.add_argument('--origin',
                        type=str,
                        help='Read location')

    parser.add_argument('--destination',
                        type=str,
                        help='Save location')

    args = parser.parse_args()

    get_full_data(args.origin, args.destination)
