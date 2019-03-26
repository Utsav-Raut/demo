import os
import datetime

def get_latest_file(dir_location):
    d={}
    for file in os.listdir(dir_location):
        t=os.path.getctime(dir_location+file) 
        d[file]=datetime.datetime.fromtimestamp(t)
        
    latest_file=max(d)
    print("last created file:",latest_file)
    return latest_file
	
	
# get_latest_file()
