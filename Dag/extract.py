"""from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

# call env vars (make the first though lol)
load_dotenv()

# sql server conn info
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DRIVER = os.getenv("DRIVER")

# this func est a connection with your SQL database


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
        return None"""
        
def extract(**kwargs):
    print("extract has succesullly ran")
    return None