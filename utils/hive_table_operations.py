def get_hive_tbl_row_count(spark, df, field_name, table_name):
    row_count_df = spark.sql(
        "select count({0}) from {1}".format(field_name, table_name)
    )
    tbl_row_count = row_count_df.head()[0]
    return tbl_row_count
