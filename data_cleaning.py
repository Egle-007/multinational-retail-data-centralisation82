from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd

class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()
        self.extractor = DataExtractor()
    # with methods to clean data from each of the data sources.
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users')
        return read_user_data
        

clean = DataCleaning()
v = clean.clean_user_data()
print(v)

        
