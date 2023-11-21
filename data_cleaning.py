from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import numpy as np
from cleaning_functions import remove_non_numerics, invalid_numbers, phone_code
# from pandasgui import show



class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()        # Connection through engine
        self.extractor = DataExtractor()            # Returns dataframe 
        
    # with methods to clean data from each of the data sources.
    def clean_user_data(self, ):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users')           # Data to clean
        sorted_user_data = read_user_data.sort_values(by='index')                               # Sorts index into a sequential order

        # Cleaning data from meaningless info
        user_data = sorted_user_data.replace('NULL', np.nan)                                    # Relaces 'NULL' into np.nan. 
        user_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val. (nan val would go through the whole row - no useful info)
        user_data = user_data[user_data['country_code'].apply(lambda x: len(str(x)) <= 3)]      # Dropping other rows with meaningless info
        
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
           
              


       


clean = DataCleaning()
v = clean.clean_user_data()
print(v)

