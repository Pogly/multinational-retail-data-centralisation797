from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import tabula

databaseconncter =  DatabaseConnector()
#engine = databaseconncter.init_db_engine()

dataExtractor = DataExtractor()
#df = dataExtractor.read_rds_table(engine,'legacy_users')

#cleandf = DataCleaning.clean_user_data(df)
#cleandf.to_csv("cleandata.csv")

#senddf = databaseconncter.upload_to_db(cleandf,'dim_users')
#pdfDataframe = dataExtractor.retrieve_pdf_data()

#cleanpdfDataframe = DataCleaning.clean_card_data(pdfDataframe)
#cleanpdfDataframe.to_csv("cleanpdfDataframe.csv")
#senddf = databaseconncter.upload_to_db(cleanpdfDataframe,'dim_card_details')

#headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#storeDataframe = dataExtractor.retrieve_stores_data(headers)
#cleanstoreDataframe = DataCleaning.called_clean_store_data(storeDataframe)
#cleanstoreDataframe.to_csv("cleanstoreDataframe.csv")

dataExtractor.extract_from_s3()

