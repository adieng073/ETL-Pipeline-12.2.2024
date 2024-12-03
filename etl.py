#from sql database to s3 with a cron joub scheduler
# TODO: Airflow changes at a later date

#import time,sqlachemy, pandas, and airflow lib
from sqlalchemy import create_engine
import boto3
from dotenv import load_dotenv
import os
#import airflow
import pandas as pd

# call env vars (make the first though lol)
load_dotenv()

# sql server conn info
SERVER = os.environ['SERVER']
DATABASE = os.environ['DATABASE']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
DRIVER = os.environ['DRIVER']

# s3 conn info
REGION = os.environ['REGION']
ACCESS_KEY = os.environ['ACCESS_KEY']
SECRET_ACCESS_KEY = os.environ['SECRET_ACCESS_KEY']
BUCKET_NAME = os.environ['BUCKET_NAME']

# this func est a connection with your SQL database
def connectToSource(sev, db, user, pw, driver):
    try:
        connectionStr = f"mssql+pyodbc://{user}:{pw}@{sev}/{db}?driver={driver}"
        engine = create_engine(connectionStr)
        print("Succesfully connected to your Source Database")
        return engine
    except Exception as e:
        print(f"Error: {e}")
        return None

# this func will take the connect as a parameter and extract data
def extractData(engine, query):
    try: 
        with engine.connect() as conn:
            df = pd.read_sql(
            sql=query,
            con=conn.connection
        )
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# this func will allow to clean your data and return cleaned data
def transformData(df):
    try:
        df = df.drop_duplicates()
        #print(df.to_string())
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# this func will take your cleaned df, create a csv with it, and upload that csv to s3 as an object
def loadData(s3Reg, s3AK, s3SAK, df, fileName):
    filePath = f"{fileName}.csv"
    df.to_csv(filePath)
    
    print("This is what is being uploaded to s3")
    print(pd.read_csv(filePath))
    
    try:
        s3 = boto3.resource(
            's3',
            region_name=s3Reg,
            aws_access_key_id=s3AK,
            aws_secret_access_key=s3SAK
        )
        print("successfully connect to s3")  
        
        s3.Bucket({BUCKET_NAME}).upload_file(filePath, filePath)
        print(f"file {filePath} has been uploaded to s3")
        os.remove(filePath)
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
    
    

if __name__ == "__main__":
    connection = connectToSource(SERVER, DATABASE, USERNAME, PASSWORD, DRIVER)
    tbl = input("what table would you like to upload: ")
    
    qry = f"SELECT * FROM [KCC].[dbo].{tbl}"
    
    # takes a connection and a query as a input 
    rawdf = extractData(connection,qry)
    
    # takes a df as an input
    cleanedDf = transformData(rawdf)
    finalDf = loadData(REGION,ACCESS_KEY,SECRET_ACCESS_KEY, cleanedDf, tbl)