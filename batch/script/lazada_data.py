from pyspark.sql.functions import (
    lower, regexp_replace, col, trim, split, size, 
    concat_ws, explode_outer, when, regexp_extract, instr, map_values, array_join, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    MapType, DoubleType, IntegerType, ArrayType
)
from pyspark.sql import SparkSession
import argparse

special_char = '[^a-z0-9A-Z_ ' \
               'àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬ' \
               'ÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]+'

spark = (SparkSession
    .builder
    .appName("lazada_full_data_v4")
    .getOrCreate())

def load_file(path):
    schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("rating_average", DoubleType(), True),
        StructField("reviews_count", IntegerType(), True),
        StructField("highlights_text", StringType(), True),
        StructField("description_text_or_html", StringType(), True),
        StructField("specifications_default", MapType(StringType(), MapType(StringType(), StringType())), True),
        StructField("total_variants", IntegerType(), True),
        StructField("variants", ArrayType(
            StructType([
                StructField("variant", StringType(), True),
                StructField("price", StringType(), True)
            ])
        ), True),
        StructField("category", StringType(), True),
        StructField("sold", StringType(), True),
        StructField("seller_name", StringType(), True)
    ])
    return spark.read.format("json").schema(schema).load(path)

def process_specifications(df):
    features_map = col("specifications_default.features")
    df = df.withColumn("specs_string", 
        when(features_map.isNotNull(), array_join(map_values(features_map), ", "))
        .otherwise(lit(None).cast("string"))
    )
    return df.drop("specifications_default")

def process_variants(df):
    # Dùng explode_outer để lặp lại thuộc tính chung cho từng variant
    df = df.withColumn("variant_item", explode_outer(col("variants")))
    df = df.withColumn("attrs", col("variant_item.variant"))
    df = df.withColumn("price_raw", col("variant_item.price"))
    return df.drop("variants", "variant_item", "total_variants")

def extract_categories_3_levels(df):
    # Giữ nguyên logic tách 3 cấp độ từ code cũ của bạn
    cat_list = split(col('category'), r"/")
    
    # Cấp 1
    df = df.withColumn('first_category', cat_list[0])
    
    # Cấp 2
    df = df.withColumn('second_category', 
        when(size(cat_list) > 1, concat_ws(' / ', cat_list[0], cat_list[1]))
        .otherwise('no info')
    )
    
    # Cấp 3
    df = df.withColumn('third_category', 
        when(size(cat_list) > 2, concat_ws(' / ', cat_list[0], cat_list[1], cat_list[2]))
        .otherwise('no info')
    )
    return df.drop("category")

def clean_sold_count(df):
    # Xử lý 1.6K -> 1600 hoặc 892 -> 892
    df = df.withColumn("sold_num", regexp_extract(col("sold"), r"([0-9.]+)", 1).cast("float"))
    df = df.withColumn("has_k", instr(lower(col("sold")), "k") > 0)
    df = df.withColumn("num_sold", 
        when(col("sold").isNull(), lit(None).cast("integer"))
        .when(col("has_k"), (col("sold_num") * 1000).cast("int"))
        .otherwise(col("sold_num").cast("int"))
    )
    return df.drop("sold_num", "has_k", "sold")

def clean_data(df):
    # Xử lý Price (Xóa ₫ và dấu chấm)
    df = df.withColumn('price', 
        when(
            col('price_raw').isNotNull(),
            regexp_replace(col('price_raw'), r'[^0-9]', '').cast('int'),
        )
        .otherwise(lit(None).cast("integer"))
    )
    
    # Làm sạch Text (Product name, Description, Highlights)
    columns_to_clean = {
        'highlights_text': 'highlights',
        'description_text_or_html': 'description',
        'product_name': 'product_name'
    }
    
    for old_col, new_col in columns_to_clean.items():
        clean_logic = lower(col(old_col))
        clean_logic = regexp_replace(clean_logic, '<.*?>', ' ') # Xóa HTML
        clean_logic = regexp_replace(clean_logic, special_char, ' ') # Xóa ký tự đặc biệt
        clean_logic = trim(regexp_replace(clean_logic, ' +', ' '))
        
        df = df.withColumn(new_col, 
            when(col(old_col).isNotNull(), clean_logic)
            .otherwise(lit(None).cast("string"))
        )
    
    return df.drop("price_raw", "description_text_or_html", "highlights_text")

def get_full_data(origin, destination):
    df = load_file(origin)
    
    # Thực hiện các bước logic
    df = process_specifications(df)
    df = process_variants(df)
    df = extract_categories_3_levels(df)
    df = clean_sold_count(df)
    df = clean_data(df)

    # Rename columns to match expected output schema
    df = (df
        .withColumnRenamed("rating_average", "avg_rating")
        .withColumnRenamed("reviews_count", "num_review")
        .withColumnRenamed("seller_name", "shop_name")
    )

    # Merge specs_string + highlights + description into one description column
    df = df.withColumn(
        "description",
        trim(regexp_replace(
            concat_ws(" ", col("specs_string"), col("highlights"), col("description")),
            " +",
            " ",
        )),
    ).drop("specs_string", "highlights")

    # Ghi file CSV
    (df.coalesce(1)
       .write.option("header", True)
       .format("csv")
       .mode('overwrite')
       .csv(destination))

    print("Succeed! Dữ liệu đã được tách 3 cấp danh mục và xử lý variant.")
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--origin', type=str)
    parser.add_argument('--destination', type=str)
    args = parser.parse_args()
    get_full_data(args.origin, args.destination)
    
    
# =========================================================================================================
#                                      CẤU TRÚC FILE ĐÍCH (FULL SCHEMA)
# =========================================================================================================
# Tổng số cột: 14
# Định dạng  : CSV (Header = True)
# ---------------------------------------------------------------------------------------------------------
# | STT | Tên Cột (Header) | Kiểu DL | Nguồn gốc / Logic xử lý                                          |
# |-----|------------------|---------|------------------------------------------------------------------|
# |  1  | product_name     | String  | Gốc (Đã làm sạch HTML & ký tự lạ)                                |
# |  2  | url              | String  | Gốc                                                              |
# |  3  | avg_rating       | Double  | Đổi tên từ rating_average                                        |
# |  4  | num_review       | Integer | Đổi tên từ reviews_count                                         |
# |  5  | shop_name        | String  | Đổi tên từ seller_name                                           |
# |  6  | attrs            | String  | Đổi tên từ variant_name (tách từ variants)                       |
# |  7  | price            | Integer | Làm sạch: Từ biến thể variants (bỏ '₫', bỏ '.'), ép kiểu số      |
# |  8  | first_category   | String  | Tách chuỗi: Cấp 1 từ cột category gốc                            |
# |  9  | second_category  | String  | Tách chuỗi: Cấp 1 + Cấp 2 từ cột category gốc                    |
# | 10  | third_category   | String  | Tách chuỗi: Cấp 1 + Cấp 2 + Cấp 3 (hoặc 'no info')               |
# | 11  | num_sold         | Integer | Đổi tên từ sold_count (tính từ sold: vd 1.6K -> 1600)            |
# | 12  | description      | String  | Ghép: specs_string + highlights + description_text_or_html (đã clean) |
# ---------------------------------------------------------------------------------------------------------