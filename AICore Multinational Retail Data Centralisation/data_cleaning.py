import pandas as pd
import numpy as np
from data_extraction import DataExtractor
import re                                                         


class DataCleaning():
    def clean_user_data(df):

        #Change "NULL" strings data type into NULL data type
        cleandf = df.replace('NULL', None)
        #Convert "join_date" column into a datetime data type
        cleandf.join_date = pd.to_datetime(cleandf.join_date,errors='coerce')
        #Remove NULL values
        cleandf = cleandf.dropna()
               
        return cleandf
    def clean_card_data(df):
         #Change "NULL" strings data type into NULL data type
        cleandf = df.replace(['NULL','N/A', 'None'] , None)
        #Remove duplacit/non-numerical card numbers
        cleandf.card_number = pd.to_numeric(cleandf.card_number,errors='coerce')
        cleandf = cleandf.drop_duplicates(subset="card_number")
        #Convert "date_payment_confirmed" column into a datetime data type
        cleandf.date_payment_confirmed = pd.to_datetime(cleandf.date_payment_confirmed,errors='coerce')
        #Remove NULL values
        cleandf = cleandf.dropna(subset=['card_number', 'expiry_date','card_provider','date_payment_confirmed'])

        return cleandf
    
    def called_clean_store_data(df):
        #Change "NULL" strings data type into NULL data type
        cleandf = df.drop(columns=['lat'])
        cleandf = cleandf.replace(['NULL','N/A', 'None'] , None)
        #Convert  "opening_date" column into a datetime data type
        cleandf.opening_date = pd.to_datetime(cleandf.opening_date,errors='coerce')
        #Strip away symbols, letters, and white spaces from "staff_number" column
        cleandf.staff_numbers = cleandf.staff_numbers.str.strip()
        cleandf.staff_numbers = cleandf.staff_numbers.str.replace(r'\D+', '')
        #Remove NULL values
        cleandf = cleandf.dropna()

        return cleandf
    
    def convert_product_weights(self,df):
        def convert_weight(value):
            match = re.match(r'(\d+\.?\d*)\s*(\w+)?', str(value))
            if not match:
                return None
            weight, unit = match.groups()
            weight = float(weight)

            if unit in ['kg']:
                return weight
            elif unit in ['g']:
                return weight / 1000
            elif unit in ['ml']:
                return weight / 1000
            else:
                return None

        df["weight"] = df["weight"].apply(convert_weight)

        return df
    
    def clean_products_data(df):
        #Convert all weight into kg units
        cleandf = DataCleaning.convert_product_weights(df)
        #Change "NULL" strings data type into NULL data type
        cleandf = df.replace(['NULL','N/A', 'None'] , None)
        #Remove NULL values
        cleandf = df.dropna()

        return df
    
    def clean_orders_data(self,df):
        #Remove unwanted columns
        cleandf = df.drop(columns=['first_name','last_name'])
        
        return cleandf

    def clean_date_time(self,df):
        # Convert columns to numeric, coercing invalid values to NaN
        columns_to_convert = ["day", "month", "year"]
        for col in columns_to_convert:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        #Change "NULL" strings data type into NULL data type
        cleandf = df.replace(['NULL','N/A', 'None'] , None)
        #Remove NULL values
        cleandf = cleandf.dropna()

