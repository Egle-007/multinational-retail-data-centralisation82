from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import numpy as np
import re
# from pandasgui import show



class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()    # Connection through engine
        self.extractor = DataExtractor()        # Returns dataframe

    # with methods to clean data from each of the data sources.
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users')           # Data to clean
        
        sorted_user_data = read_user_data.sort_values(by='index')                               # Sorts index into a sequential order

        # Cleaning data from meaningless info
        user_data = sorted_user_data.replace('NULL', np.nan)                                    # Relaces 'NULL' into np.nan. 
        user_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val. (nan val would go through the whole row - no useful info)
        user_data = user_data[user_data['country_code'].apply(lambda x: len(str(x)) <= 3)]      # Dropping other rows with meaningless info
        
        user_data['country_code'] = user_data['country_code'].str.replace('GGB', 'GB')          # Checked unique country codes: np.sort(nan_user_data["country_code"].unique()), only Country_code typo 'GGB' to be converted into 'GB'


        # Converting dTypes
        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string') 
        # user_data['phone_number'] = user_data['phone_number'].astype('string')

        user_data['date_of_birth'] = user_data['date_of_birth'].apply(parse) 
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], infer_datetime_format=True, errors='coerse')
        
        user_data['join_date'] = user_data['join_date'].apply(parse) 
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], infer_datetime_format=True, errors='coerse')
            

        # Cleaning phone numbers
        user_data['phone_number'] = user_data['phone_number'].str.replace('+49','')     
        user_data['phone_number'] = user_data['phone_number'].str.replace('+44','')
        user_data['phone_number'] = user_data['phone_number'].str.replace('+1','')

        def remove_non_numerics(x):                                                             # Cleans phone number from nondigits, str.replace('[^0-9]') did not work 
            return re.sub('[^0-9]', '', x) 

        def invalid_numbers(x):                                                                 # UK, US, Germany phone numbers consist of 10 digits, sometimes written with 0 in the front making it 11 digits in total.
            if len(str(x)) >= 10 and len(str(x)) <= 11:                                         # Function 'invalid_numbers' picks those numbers that are outside expected length and returns 'Invalid number' instead.
                return x
            else:
                return 'Invalid number'
        
        user_data['phone_number'] = user_data['phone_number'].apply(remove_non_numerics, invalid_numbers)
            
        def del_zero(x):
            if len(str(x)) == 11 and str(x[0]) == '0':
                return x[1:]
            else:
                return x
        
        user_data['phone_number'] = user_data['phone_number']. apply(del_zero)

        def phone_code(x):
            if x == 'GB':
                return '+44'
            elif x == 'US':
                return '+1'
            else:
                return '+49'

        user_data['phone_country_code'] = user_data['country_code'].apply(phone_code)           # Building a new column for phone country code

        col = user_data.pop('phone_country_code')                                               # Moving 'phone_country_code' to logical plase in the table
        user_data.insert(8, col.name, col)

        return user_data
           
              


       


clean = DataCleaning()
v = clean.clean_user_data()
print(v)



     # user_data['phone_number'] = user_data['phone_number'].apply(remove_non_numerics)

        # df = np.sort(user_data['phone_number'].unique()) 
        # phone_code = np.sort(user_data["country_code"].unique()) 
        # print(phone_code)

 # def drop_nonsence_rows():
        #     for row in range(len(nan_user_data["country_code"])):
        #         if len(nan_user_data["country_code"][row]) > 3:
        #             nan_user_data.drop([row])
        #     # for i in nan_user_data['country_code']:
        #     #     if len(i) > 3:
        #     #         nan_user_data.drop([i])

           # nan_user_data.drop(nan_user_data[len(nan_user_data['country_code']) >= 4].index)
        


        # user_data['phone_number'] = user_data['phone_number'].astype('string')
        # df = user_data['phone_number'].apply(remove_non_numberics)      
        # nan_user_data['phone_number'] = nan_user_data['phone_number'].str.replace('[^0-9+]', '') # this should replace everything that is not 0-9 but it doesnt
        # # user_data['phone_number'] = user_data['phone_number'].str.strip('.+)(|')
        # # print(user_data.info())