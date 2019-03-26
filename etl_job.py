from dependencies.spark import start_spark
from utils.row_count_validation import get_row_count_diff

from flask import Flask, jsonify, request
app = Flask(__name__)

# @app.route('/nikeDirect/rowCountValidation/getTargetTables', methods=['GET'])
def main():
    """Main ETL script definition.
    :return: None
    """
    # Start spark application and get spark session, logger and config
    spark, log, config = start_spark(
        app_name = 'nike_etl_job',
        files = ['configs/etl_config.json']
    )
    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # log the success and terminate the spark session
    log.warn('test_etl_job is finished')
    # spark.stop()      # Need to implement hooks
    return spark, log, config
    # return None

def execute_etl_job(db_name):
    spark, log, config = main()
    # Execute ETL pipeline
    data = extract_data(spark, db_name, config['table_name'])
    data_transformed = transformed_data(data, spark, config['table_name'], config['file_path'], 
                        config['field_name'])
    load_data(data_transformed, config['table_name'])
    return data_transformed

def list_of_tables(db_name):
    spark, log, config = main()
    list_of_tables = []
    spark.sql("use {0}".format(db_name)) # This is a hive db
    df = spark.sql("show tables")
    df.show()
    row_df = df.select('tableName').collect()   #This gives a list of spark row objects
    print(row_df)
    print(len(row_df))
    for i in range(0, len(row_df)):
        list_of_tables.append(row_df[i].__getitem__('tableName'))   #to extract the value from a row object use the row dataframe.__getitem__('tablename')
    return list_of_tables

def extract_data(spark, db_name, table_name):
    """Load data from Hive table"""
    spark.sql("use {0}".format(db_name)) # This is a hive db
    df = spark.sql(
        "select * from {0}".format(table_name)
    )
    return df

def transformed_data(df, spark, table_name, file_path, field_name):
    "Transform the original data set"
    row_count_diff, result, hive_tbl_row_count, input_file_row_count = get_row_count_diff(spark, df, field_name, table_name, file_path)
    print("Hive table row count = {}".format(hive_tbl_row_count))
    print("Input file row count = {}".format(input_file_row_count))
    print("Row count difference = {}".format(row_count_diff))
    print("The result is {}".format(result))
    df_transformed = [row_count_diff, result, hive_tbl_row_count, input_file_row_count]
    return df_transformed

def load_data(df_transformed, table_name):
    print('Hello')

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    # app.run(debug=True)
