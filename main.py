import boto3
from pyspark.sql import SparkSession
import kagglehub
import os
from dotenv import load_dotenv

# Initialize S3 client
s3 = boto3.client('s3')

# S3 landing information
s3_bucket_name = "bucketbyamadou"
s3_folder_name = "IMDB/"
folderPath = r"C:\Users\adien\.kaggle\CSE 460 database"

# create a spark session 
def sparkSession():
    spark = SparkSession.builder.appName("end to end pipeline")
    spark.getOrCreate()
    return spark
    
    
# read the differnent file types 
def readFiles(folderPath):
    fileExt = os.path.splitext(folderPath)[1]
    
    for file in folderPath:
        if os.path.isfile(file):
             if fileExt == ".txt":
                print("txt file")
             elif fileExt == ".csv":
                print("csv file")
             elif fileExt == ".json":
                print("json file")
        elif os.path.isdir(file):
            pass
            
    
# write to s3 
def uplaodToS3():
    #to-do uplad the file to s3
    pass

# main func
if __name__ == "__main__":
    readFiles(folderPath)

