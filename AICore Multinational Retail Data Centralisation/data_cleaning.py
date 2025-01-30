import pandas as pd
import numpy as np
from data_extraction import DataExtractor
import re                                                       


class DataCleaning():
    def clean_user_data(self,dataFrame):
        #Convert "join_date" column into a datetime data type
        dataFrame.join_date = pd.to_datetime(dataFrame.join_date,format='mixed',errors='coerce')
        #Change "NULL" strings data type into NULL data type
        dataFrame = dataFrame.replace(['NULL'], None)
        #Remove NULL values
        dataFrame = dataFrame.dropna()
        return dataFrame

    def clean_card_data(self,dataFrame):
         #Change "NULL" strings data type into NULL data type
        dataFrame = dataFrame.replace(['NULL','N/A', 'None'] , None)
        #Remove no numeric values
        dataFrame['card_number'] = dataFrame['card_number'].astype(str).str.strip()
        dataFrame['card_number'] = dataFrame['card_number'].str.replace(r'\D+', '', regex=True)
        # Remove duplicate card numbers
        dataFrame = dataFrame.drop_duplicates(subset="card_number")
        #Convert "date_payment_confirmed" column into a datetime data type
        dataFrame.date_payment_confirmed = pd.to_datetime(dataFrame.date_payment_confirmed,format='mixed',errors='coerce')
        #Remove NULL values
        dataFrame = dataFrame.dropna(subset=['card_number', 'expiry_date','card_provider','date_payment_confirmed'])
        return dataFrame
    
    def called_clean_store_data(self,dataFrame):
        #Change "NULL" strings data type into NULL data type
        dataFrame = dataFrame.drop(columns=['lat'])
        #Convert  "opening_date" column into a datetime data type
        dataFrame.opening_date = pd.to_datetime(dataFrame.opening_date,errors='coerce')
        #Strip away symbols, letters, and white spaces from "staff_number" column
        dataFrame.staff_numbers = dataFrame['staff_numbers'].str.replace(r'[^0-9]', '', regex=True)
        dataFrame.staff_numbers = dataFrame.staff_numbers.str.strip()
        dataFrame.staff_numbers = pd.to_numeric(dataFrame.staff_numbers,errors='coerce')
        #Clear Nulls in storecode
        dataFrame.store_code = dataFrame.store_code.replace(['NULL','N/A', 'None'] , None)
        dataFrame = dataFrame.dropna(subset=['store_code'])
        return dataFrame
    
    def convert_product_weights(self,dataFrame):
        def convert_weight(value):
            multy = re.match(r'(\d+)\s*[xX]\s*(\d+\.?\d*)\s*(\w+)?',str(value))
            if multy:
                numb1, numb2, unit = multy.groups()
                weight = float( float(numb1)* float(numb2))
            else:

                match = re.match(r'(\d+\.?\d*)\s*(\w+)?', str(value))
                if match:
                    weight, unit = match.groups()
                    weight = float(weight)
                else:
                    return None


                weight, unit = match.groups()
                weight = float(weight)

            if unit in ['kg']:
                return weight
            elif unit in ['g']:
                return weight / 1000
            elif unit in ['ml']:
                return weight / 1000
            elif unit in ['oz']:
                return weight / 35.274
            else:
                return None

        dataFrame["weight"] = dataFrame["weight"].apply(convert_weight)

        return dataFrame
    
    def clean_products_data(self,dataFrame):
        #Convert all weight into kg units
        dataFrame = self.convert_product_weights(dataFrame)
        #Change "NULL" strings data type into NULL data type
        dataFrame = dataFrame.replace(['NULL','N/A', 'None'] , None)
        #Remove NULL values
        dataFrame = dataFrame.dropna()

        return dataFrame
    
    def clean_orders_data(self,dataFrame):
        #Remove unwanted columns
        dataFrame = dataFrame.drop(columns=['first_name','last_name','level_0'])
        
        return dataFrame

    def clean_date_time(self,dataFrame):
        # Convert columns to numeric, coercing invalid values to NaN
        columns_to_convert = ["day", "month", "year"]
        for col in columns_to_convert:
            dataFrame[col] = pd.to_numeric(dataFrame[col], errors='coerce')
        #Change "NULL" strings data type into NULL data type
        dataFrame = dataFrame.replace(['NULL','N/A', 'None'] , None)
        #Remove NULL values
        dataFrame = dataFrame.dropna()

        return dataFrame

