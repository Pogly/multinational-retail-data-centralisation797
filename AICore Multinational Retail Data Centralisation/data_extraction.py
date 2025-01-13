import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from io import StringIO  


class DataExtractor():

    def read_rds_table(self,engine,tableName):
        dataframe = pd.read_sql_table('legacy_users', engine)
        return dataframe
    
    def retrieve_pdf_data(self):
        pdf_path =  'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pdfDataFrame = pd.concat(tabula.read_pdf(pdf_path, pages="all",stream=True))

    def list_number_of_stores(self,header):

        response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers=header)
        if response.status_code == 200:
            data = response.json()
            numberofstores = data['number_stores']
            numberofstores = int(numberofstores)
            return numberofstores
    
    def retrieve_stores_data(self,header):
        numberofstores = self.list_number_of_stores({'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        for number in range(numberofstores):
            response = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{number}',headers=header)
            data = response.json()
            if number == 0:
                 storeDataFrame = pd.DataFrame(data,index=[number])
            else:
                additional_data = pd.DataFrame([data], columns=storeDataFrame.columns)
                storeDataFrame = pd.concat([storeDataFrame, additional_data], ignore_index=True)
        return storeDataFrame
    
    def extract_from_s3(self):
        bucket = 'data-handling-public'
        filekey = 'products.csv'

        s3 = boto3.client('s3')
        #obj = s3.download_file(bucket, filekey, r"C:\Users\benja\Documents\AICore Test Code\AICore Multinational Retail Data Centralisation\products.csv")
        s3_object = s3.get_object(Bucket=bucket, Key=filekey)
        s3_data = s3_object['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(s3_data))
        print(df)
        