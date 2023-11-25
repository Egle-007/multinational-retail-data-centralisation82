from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import numpy as np
from cleaning_functions import remove_non_numerics, invalid_numbers, phone_code
# from pandasgui import show
from sqlalchemy import create_engine
from datetime import datetime




class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()        # Connection through engine
        self.extractor = DataExtractor()            # Returns dataframe 
        
    # with methods to clean data from each of the data sources.
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector, 'legacy_users')          # Data to clean
        sorted_user_data = read_user_data.sort_values(by='index')                               # Sorts index into a sequential order

        # Removing meaningless info
        user_data = sorted_user_data.replace('NULL', np.nan)                                    # Relaces 'NULL' with np.nan. 
        user_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val. (nan val would go through the whole row - no useful info)
        user_data = user_data[user_data['country_code'].apply(lambda x: len(str(x)) <= 3)]      # Dropping other rows with meaningless info based on the expected length of a string
        
        user_data['country_code'] = user_data['country_code'].str.replace('GGB', 'GB')          # Converts country_code typo 'GGB' into 'GB'
        user_data['address'] = user_data['address'].str.replace('\n', ', ' )                    # Replaces '\n' from the data in the address column with a ','.

        # Assigning columns to an appropriate dTypes
        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string') 
        
        user_data['date_of_birth'] = user_data['date_of_birth'].apply(parse) 
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], infer_datetime_format=True, errors='coerse')
        
        user_data['join_date'] = user_data['join_date'].apply(parse) 
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], infer_datetime_format=True, errors='coerse')
            
        # Cleaning phone numbers
        user_data['phone_number'] = user_data['phone_number'].str.replace('+49','')             # Removes phone country code (if it is in the number) for numbers to be in the same style. 
        user_data['phone_number'] = user_data['phone_number'].str.replace('+44','')
        user_data['phone_number'] = user_data['phone_number'].str.replace('+1','')
       
        user_data['phone_number'] = user_data['phone_number'].apply(remove_non_numerics)        # Cleans phone number from nondigits.
        user_data['phone_number'] = user_data['phone_number']. apply(invalid_numbers)           # Returns either 10 digit numbers or an 'invalid number' for those numbers that doesn't meet the criterion.
        user_data['phone_country_code'] = user_data['country_code'].apply(phone_code)           # Creates a separate column for phone country code.                                                        

        col = user_data.pop('phone_country_code')                                               # Moves 'phone_country_code' column to a logical plase in the table.
        user_data.insert(8, col.name, col)

        return user_data
    
    def upload_to_db(self, df, table_name):                                                     # Creates connection with and uploads dataframe to local pgadmin sales_data database
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = input()
        DATABASE = 'sales_data'
        PORT = 5432
        engine_local = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, engine_local)

    def clean_card_data(self):
        card_data = self.extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
       
        # Removing meaningless info
        card_data = card_data.replace('NULL', np.nan)                                           # Relaces 'NULL' with np.nan. 
        card_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val.
        card_data = card_data[card_data['expiry_date'].apply(lambda x: len(str(x)) <= 5)]       # Dropping other rows with meaningless info based on the expected length of a string. Checked with .unique().
        
        card_data['card_number'] = card_data['card_number'].astype('string')                    # To remove '?' appearing in card numbers, first converts them to strings 
        card_data['card_number'] = card_data['card_number'].apply(remove_non_numerics)          # then applies remove_non_numerics. After using the function, dType turns back into object.

        # Assigning columns to an appropriate dTypes
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].apply(parse) 
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], infer_datetime_format=True, errors='coerse')
       
        card_data['expiry_date'] = '01/' + card_data['expiry_date'].astype(str)
        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], format='mixed')
       
        # Sorting values
        card_data.sort_values(by='date_payment_confirmed', ascending = False, inplace = True)   # Sorts values based on the date when the paiment was confirmed in descending order
        card_data = card_data.reset_index(drop=True)                                            # Fixes index, was a repeat of 54.
       
        return card_data
    
    def called_clean_store_data(self):
        # stores_data = self.extractor.retrieve_stores_data()
        # stores_data.to_csv('stores_data.csv')

        stores_data = pd.read_csv('stores_data.csv')
        # print(type(stores_data))
        stores_data.set_index('index', inplace=True)

        unique_country_codes = stores_data['country_code'].unique()
        # print(unique_country_codes)

        stores_data = stores_data[stores_data['country_code'].apply(lambda x: len(str(x)) <= 3)]
        # print(stores_data)
        unique_country_codes = stores_data['country_code'].unique()
        # print(unique_country_codes)

        unique_lat = stores_data['lat'].unique()
        # print(unique_lat)

        stores_data.drop('lat', axis=1, inplace=True)
        # print(stores_data.info())

        


        # return np.sort(stores_data['country_code'].unique())
        # stores_data = stores_data[stores_data['country_code'].apply(lambda x: len(str(x)) <= 3)]
        # sf_sal.drop("Status", axis=1, inplace=True)
        # .drop_duplicates()



        

        
# card_number	expiry_date	card_provider	date_payment_confirmed           
       


clean = DataCleaning()
# v = clean.clean_user_data()
# print(v)
# h = clean.clean_card_data()
# print(h)
# z = clean.upload_to_db(h, 'dim_card_details')
s = clean.called_clean_store_data()


        # card_data.to_csv('card_data.csv')
