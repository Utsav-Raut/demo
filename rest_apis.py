from utils import row_count_validation as r, latest_file as latest
import etl_job
from flask import Flask, request, jsonify
app = Flask(__name__)


@app.route('/nikeDirect/rowCountValidation/getTargetTables/', methods=['GET'])
def get_hive_tables():
    hive_db_name = request.args.get('hiveDBname')
    list_of_hive_tbls = etl_job.list_of_tables(hive_db_name)
    return jsonify({'Tables':list_of_hive_tbls})

@app.route('/nikeDirect/rowCountValidation/getLatestZIPFile/', methods=['GET'])
def get_s3_location():
    s3_folder_loc = request.args.get('s3FolderLocation')
    latest_zip_file = latest.get_latest_file(s3_folder_loc)
    return jsonify({'Latest Zip File':latest_zip_file})

@app.route('/nikeDirect/rowCountValidation/getRowCountDiff/', methods=['GET'])
def getRowCountVal():
    db_name = request.args.get('hiveDBname')
    print(db_name)
    data_transformed = etl_job.execute_etl_job(db_name)
    row_count_diff_data = {}
    row_count_diff_data["NumberOfTransactionsInFile"] = data_transformed[3]
    row_count_diff_data["NumberofTransactionsInTargetTable"] = data_transformed[2]
    row_count_diff_data["Difference"] = data_transformed[0]
    return jsonify({'Row_Count Diff':row_count_diff_data})


if __name__ == '__main__':
    app.run(debug=True)

