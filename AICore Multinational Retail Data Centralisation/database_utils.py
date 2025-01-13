import yaml
import sqlalchemy as sq
import pandas as pd
import psycopg2
from sklearn.datasets import load_iris

class DatabaseConnector():

    def read_db_creds(self):
        with open('db_creds.yaml','r') as C:
            data_credentials = yaml.safe_load(C)
        
        return data_credentials
    
    def init_db_engine(self):

        host = self.read_db_creds()["RDS_HOST"]
        password = self.read_db_creds()["RDS_PASSWORD"]
        user = self.read_db_creds()["RDS_USER"]
        port = self.read_db_creds()["RDS_PORT"]
        database = self.read_db_creds()["RDS_DATABASE"]

        engine = sq.create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()

        engine.connect()
        inspector = sq.inspect(engine,)
        tablenames = inspector.get_table_names()
        engine.close()

        return tablenames
    
    def upload_to_db(self,df,tablename):
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Mang0ANDice!'
        DATABASE = 'sales_data'
        PORT = 5432
        sdengine = sq.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        sdengine.connect()

        df.to_sql(tablename, sdengine, if_exists='replace')
    