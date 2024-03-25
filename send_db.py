import os
import pymysql
import sqlalchemy
from sqlalchemy import create_engine

database_username= os.environ['database_username']
database_password= os.environ['database_password']
database_ip= os.environ['database_ip']
database_name= os.environ['database_name']

def send_db(data,table,option):
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name),echo=False)
    
    data.to_sql(con=engine, name=table, if_exists=option,index=False)
    
    
    engine.dispose()