from database_utils import DatabaseConnector
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd
import tabula
import requests
# from pandasgui import show
from config import header, endpoint_number, endpoint_store

class DataExtractor:
         
    def read_rds_table(self, instance, table_name):
        engine = instance.init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col='index')  # index_col='index'
        df_copy = df.copy()
        return df_copy
    
    def  retrieve_pdf_data(self, link):
        pdf_path = link
        df_pdf = tabula.read_pdf(pdf_path, stream=False, pages='all')
        df_pdf = pd.concat(df_pdf)
        return df_pdf

    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        if response.status_code == 200:
            data = response.json()
            number = data['number_stores']
            return number
        else:
            return 'Error'

    def retrieve_stores_data(self):
        store_numbers = list(range(1, self.list_number_of_stores(endpoint_number, header)+1)) 
        
        stores_data = []
        for store_number in store_numbers:
            response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'.format(store_number))
            if response.status_code == 200:
                stores_data.append(response.json())
            else:
                print('Error')
        
        return stores_data
        #     'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

        

con = DatabaseConnector()
extractor = DataExtractor()
# extractor.read_rds_table(con, "legacy_users")

# extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# extractor.list_number_of_stores(endpoint_number, header)
extractor.retrieve_stores_data()




# ['first_name', 'last_name', 'date_of_birth', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'join_date', 'user_uuid']
# /Users/eglute/Desktop/AiCore/retail_project/card_details.pdf