from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import tabula

databaseconncter =  DatabaseConnector()
dataExtractor = DataExtractor()
dataCleaning = DataCleaning()


databaseconncter.read_db_creds()
engine = databaseconncter.init_db_engine()

#print table list
#print(databaseconncter.list_db_tables(engine))

#Legacy Uesers Data
#df = dataExtractor.read_rds_table(engine,'legacy_users')
#cleandf = dataCleaning.clean_user_data(df) 
#cleandf.to_csv("cleanUeserData.csv")
#senddf = databaseconncter.upload_to_db(cleandf,'dim_users')

# #Card Details Data
# pdfDataframe = dataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# pdfDataframe.to_csv("CardDetailsDataframe.csv")
# cleanpdfDataframe = dataCleaning.clean_card_data(pdfDataframe)
# cleanpdfDataframe.to_csv("cleanCardDetailsDataframe.csv")
# senddf = databaseconncter.upload_to_db(cleanpdfDataframe,'dim_card_details')

# #Store Data
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
storeDataframe = dataExtractor.retrieve_stores_data(headers)
cleanstoreDataframe = dataCleaning.called_clean_store_data(storeDataframe)
cleanstoreDataframe.to_csv("cleanstoreDataframe.csv")
storeDataframe.to_csv("storeDataframe.csv")
senddf = databaseconncter.upload_to_db(cleanstoreDataframe,'dim_store_details')

#Product Data
# itemDataframe = dataExtractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/products.csv')
# itemDataframe.to_csv("itemDataframe.csv")
# cleanitemdataframe = dataCleaning.clean_products_data(itemDataframe)
# cleanitemdataframe.to_csv("cleanitemDataframe.csv")
# senddf = databaseconncter.upload_to_db(cleanitemdataframe,'dim_products')

#Order Table Data
#OrderData = dataExtractor.read_rds_table(engine,'orders_table')
#cleanOrderData = dataCleaning.clean_orders_data(OrderData)
#cleanOrderData.to_csv("cleanOrderData.csv")
#senddf = databaseconncter.upload_to_db(cleanOrderData,'orders_table')

#Date Time Data
#dateTimeDataframe = dataExtractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
#cleanDateTimeDataframe = dataCleaning.clean_date_time(dateTimeDataframe)
#cleanDateTimeDataframe.to_csv("cleanDateTimeDataframe.csv")
#cleanDateTimeDataframe = databaseconncter.upload_to_db(cleanDateTimeDataframe,'dim_date_times')

print("done")