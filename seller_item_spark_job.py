from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import percent_rank
from pyspark.sql import SparkSession

aws_access_key = "r7LX3wSCP5ZK1yXupKEVVG"
aws_secret_key = "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw"
s3_endpoint_url = "https://hb.vkcloud-storage.ru"
s3_bucket = "s3a://startde-raw/"


spark = SparkSession.builder.appName('abc'). \
        config("spark.hadoop.fs.s3a.access.key", aws_access_key). \
        config("spark.hadoop.fs.s3a.secret.key", aws_secret_key). \
        config("fs.s3a.endpoint", s3_endpoint_url).  \
        getOrCreate()

raw_items_df = spark.read.parquet(
    "s3a://startde-raw/raw_items",
    header=True,
    inferSchema=True
)

w = Window.orderBy("item_rate")

raw_w_avg_sales = raw_items_df.withColumn('returned_items_count', 
                        (raw_items_df['ordered_items_count'] - (raw_items_df['ordered_items_count']*(raw_items_df['avg_percent_to_sold']/100))). \
                        cast(IntegerType())). \
             withColumn('potential_revenue', 
                        (raw_items_df['availability_items_count'] + raw_items_df['ordered_items_count']) * raw_items_df['item_price']). \
            withColumn('total_revenue', 
                        raw_items_df['goods_sold_count'] * raw_items_df['item_price']). \
            withColumn('avg_daily_sales', raw_items_df['goods_sold_count']/raw_items_df['days_on_sell'])

final = raw_w_avg_sales. \
            withColumn('days_to_sold', raw_w_avg_sales['availability_items_count']/raw_w_avg_sales['avg_daily_sales']). \
            withColumn("item_rate_percent", percent_rank().over(w))
            
final.write.parquet(
    "s3a://startde-project/na-tarasova/seller_items"
)

spark.stop()