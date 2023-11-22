from database_utils import DatabaseConnector
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd
import tabula
# from pandasgui import show

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


con = DatabaseConnector()
extractor = DataExtractor()
extractor.read_rds_table(con, "legacy_users")

extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')




# ['first_name', 'last_name', 'date_of_birth', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'join_date', 'user_uuid']
# /Users/eglute/Desktop/AiCore/retail_project/card_details.pdf