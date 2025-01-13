import pandas as pd
import numpy as np
from data_extraction import DataExtractor                                                                       


class DataCleaning():
    def clean_user_data(df):

        #Change "NULL" strings data type into NULL data type
        cleandf = df
        cleandf = cleandf.replace('NULL', None)
        #Convert "join_date" column into a datetime data type
        cleandf.join_date = pd.to_datetime(cleandf.join_date,errors='coerce')
        #Remove NULL values
        cleandf = cleandf.dropna()
               
        return cleandf
    def clean_card_data(df):
         #Change "NULL" strings data type into NULL data type
        cleandf = df
        cleandf = cleandf.replace(['NULL','N/A', 'None'] , None)
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
        cleandf = df
        cleandf = cleandf.drop(columns=['lat'])
        cleandf = cleandf.replace(['NULL','N/A', 'None'] , None)
        #Convert  "opening_date" column into a datetime data type
        cleandf.opening_date = pd.to_datetime(cleandf.opening_date,errors='coerce')
        #Strip away symbols, letters, and white spaces from "staff_number" column
        cleandf.staff_numbers = cleandf.staff_numbers.str.strip()
        cleandf.staff_numbers = cleandf.staff_numbers.str.replace(r'\D+', '')
        #Remove NULL values
        cleandf = cleandf.dropna()

        return cleandf



