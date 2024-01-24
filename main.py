import os
from dotenv import load_dotenv
load_dotenv()
#from src.extract import connect_to_redshift
from datetime import datetime
from src.extract import extract_transactional_data
from src.transform import identify_and_remove_duplicates
from src.load_data_to_s3 import df_to_s3
start_time = datetime.now()

#reading the variables from env file
dbname = os.getenv("dbname")
port = os.getenv("port")
host = os.getenv("host")
user_name = os.getenv("user")
password = os.getenv("password")

#connect_to_redshift(dbname, host, port, user_name, password)
#Step 1 Extract data
ot_transformed =extract_transactional_data(dbname, host, port, user_name, password)
#print(online_transactions_data.head())

#Step2 identify and remove duplicates
ot_wout_duplicates = identify_and_remove_duplicates(ot_transformed)

#Step 3 load data to s3
key="transformations_final/ra_online_trans_transformed.pkl"
s3_bucket="sep-bootcamp"
aws_access_key_id=os.getenv("aws_access_key_id")
aws_secret_access_key=os.getenv("aws_secret_access_key_id")

df_to_s3(ot_wout_duplicates, key, s3_bucket, aws_access_key_id, aws_secret_access_key)

# if you want to you can calculate
execution_time = datetime.now() - start_time
print(f"Total execution time (hh:mm:ss) {execution_time}")


