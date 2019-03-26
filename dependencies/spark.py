import __main__
from os import listdir, environ, path
import json
import findspark
findspark.init()
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging
def start_spark(app_name = 'my_spark_app', master = 'local[*]', jar_packages = [],
                files = [], spark_config = {}):

                # detect execution environment
                flag_repl = not(hasattr(__main__, '__file__'))
                flag_debug = 'DEBUG' in environ.keys()

                if not(flag_repl or flag_debug):
                    # get spark session factory
                    spark_builder = (
                        SparkSession
                        .builder
                        .appName(app_name))
                else:
                    # get spark session factory
                    spark_builder = (
                        SparkSession
                        .builder
                        .master(master)
                        .appName(app_name)
                    )

                    # Create Spark JAR packages string
                    spark_jars_packages = ','.join(list(jar_packages))
                    spark_builder.config('spark.jars.packages', spark_jars_packages)

                    spark_files = ','.join(list(files))
                    spark_builder.config('spark.files', spark_files)

                    # add other config parameters
                    for key, val in spark_config.items():
                        spark_builder.config(key, val)

                # create session and retrieve spark logger object
                # driver_location = "D:/Users/URaut/Program_Files/hive/lib"
                # spark_sess = spark_builder.config("spark.driver.extraClassPath",driver_location).enableHiveSupport().getOrCreate()
                spark_sess = spark_builder.enableHiveSupport().getOrCreate()
                spark_logger = logging.Log4j(spark_sess)

                # get config file if sent to cluster with --files
                # spark_files_dir = SparkFiles.getRootDirectory()
                # config_files = [filename
                #                 for filename in listdir(spark_files_dir)
                #                 if filename.endswith('config.json')]

                # if config_files:
                #     path_to_config_file = path.join(spark_files_dir, config_files[0])
                #     with open(path_to_config_file, 'r') as config_file:
                #         config_dict = json.load(config_file)
                #     spark_logger.warn('loaded config from ' + config_files[0])
                # else:
                #     spark_logger.warn('no config file found')
                #     config_dict = None

                path_to_config_file = "D:/Users/URaut/Videos/ETL-master/nike/configs/etl_config.json"
                with open(path_to_config_file, 'r') as config_file:
                        config_dict = json.load(config_file)

                return spark_sess, spark_logger, config_dict

