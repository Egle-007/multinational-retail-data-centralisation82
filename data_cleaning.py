from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd

class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()    # Where are we getting data from
        self.extractor = DataExtractor()        # Returns df_copy
    # with methods to clean data from each of the data sources.
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users')
        print(read_user_data.dtypes)
        print(read_user_data.describe())
        

        

clean = DataCleaning()
v = clean.clean_user_data()
print(v)


# Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
# You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows 
# filled with the wrong information.
