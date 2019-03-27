import zipfile
import json
import pprint
from utils import latest_file

def get_file_row_count(s3_zip_folder_location, field_name):
	# file_name = "D:/Users/URaut/Desktop/s3folderlocation/f3.zip"
	latest_zip_file = latest_file.get_latest_file(s3_zip_folder_location)
	transaction_ids = []
	with zipfile.ZipFile(s3_zip_folder_location + latest_zip_file) as z:
		for filename in z.namelist():
			with z.open(filename) as f:
				for line in f:
					# data = line.read()
					d = json.loads(line.decode("utf-8"))
					# print(d["notifications"][0]["POSLog"]["Transaction"]["TransactionID"]) #print the transaction ids
					transaction_ids.append(d["notifications"][0]["POSLog"]["Transaction"]["TransactionID"]) #inserting the transactions ids in a list
		count_of_files_in_zip = len(z.namelist())	# Gives the total number of files in the zip
		total_transaction_id_count = len(transaction_ids)	
	return total_transaction_id_count



# import findspark
# findspark.init()
# from pyspark.sql import SparkSession

# def get_file_row_count(file_path, field_name):
#     # spark = SparkSession.builder.appName("Read File Data").getOrCreate()
#     # read_file_df = spark.read.json(file_path)
#     # tra_list= read_file_df.select(field_name).collect()
#     # return len(tra_list)
