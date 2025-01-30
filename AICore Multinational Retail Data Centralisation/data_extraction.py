import pandas as pd
import tabula
import requests
import io  
import json
import csv


class DataExtractor():

    def read_rds_table(self,engine,tableName):
        dataFrame = pd.read_sql_table(tableName, engine)
        return dataFrame
    
    def retrieve_pdf_data(self,html):
        pdf_path = html
        dataFrame = pd.concat(tabula.read_pdf(pdf_path, pages="all"))
        return dataFrame
    

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
    
    def extract_from_s3(self,url):
        #Find S3 bucket
        headers = {'Host': 'data-handling-public.s3-eu-west-1.amazonaws.com'}
        request = requests.get(url, headers=headers).content

        #Read Csv Files
        try:
            df = pd.read_csv(io.StringIO(request.decode('utf-8')))
            return df
        except:
            print('notCSV')
            pass
        #Read Json Files
        try:
            json.loads(request)
            df = pd.read_json(io.StringIO(request.decode('utf-8')))
            return df
        except:
            print('notJSON')
            pass
        
        