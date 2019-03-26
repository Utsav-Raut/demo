import findspark
findspark.init()
from pyspark.sql import SparkSession

def get_file_row_count(file_path, field_name):
    spark = SparkSession.builder.appName("Read File Data").getOrCreate()
    read_file_df = spark.read.json(file_path)
    tra_list= read_file_df.select(field_name).collect()
    return len(tra_list)
