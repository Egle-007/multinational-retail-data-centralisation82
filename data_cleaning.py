from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import numpy as np
# from pandasgui import show



class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()    # Where are we getting data from
        self.extractor = DataExtractor()        # Returns df_copy
    # with methods to clean data from each of the data sources.
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users') # Data to clean

        # noticed the index was not in sequential order, so sorted it
        sorted_user_data = read_user_data.sort_values(by='index')

        # Came across 'NULL' strings, so converted it into np.nan. All columns had 21 nan, therefore dropped all rows with nan.  
        nan_user_data = sorted_user_data.replace('NULL', np.nan)
        # is_null = nan_user_data.isnull().sum() - used it to check number of nan's.
        nan_user_data.dropna(axis=0, inplace=True)

        # getting rid of meaningless rows
        user_data = nan_user_data[nan_user_data['country_code'].apply(lambda x: len(str(x)) <= 2)]

        # converting dTypes
        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string') 

        user_data['date_of_birth'] = user_data['date_of_birth'].apply(parse) 
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], infer_datetime_format=True, errors='coerse')
        user_data['join_date'] = user_data['join_date'].apply(parse) 
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], infer_datetime_format=True, errors='coerse')

        csv = user_data.to_csv('updated_user_date.csv')

        # date_format = "mixed"
        # pd.to_datetime(user_data['date_of_birth'], format=date_format)
        # pd.to_datetime(user_data['join_date'], format=date_format)
        # print(nan_user_data)
        # print(row_752)
        return csv
    
#        mixed_date_df['dates'] = mixed_date_df['mixed_dates'].apply(parse)
# mixed_date_df['dates'] = pd.to_datetime(mixed_date_df['dates'], infer_datetime_format=True, errors='coerce')


# np.sort(auto_df["stroke"].unique())
 
# read_user_data['DOB'] = read_user_data['date_of_birth'].apply(parse)
# read_user_data['DOB'] = pd.to_datetime(read_user_data['DOB'], infer_datetime_format=True, errors='coerce')
# read_user_data.join_date = read_user_data.join_date.astype('datetime64') 
# read_user_data.date_of_birth = read_user_data.date_of_birth.astype('datetime64')         
        
# age_df.Name = age_df.Name.astype('string') datetime64
# .phone_number = .phone_number.astype('int64', errors='ignore')
# age_df.Age = pd.to_numeric(age_df.Age, errors='coerce')

# mixed_date_df['dates'] = mixed_date_df['mixed_dates'].apply(parse)
# mixed_date_df['dates'] = pd.to_datetime(mixed_date_df['dates'], infer_datetime_format=True, errors='coerce')


       

clean = DataCleaning()
v = clean.clean_user_data()
print(v)

