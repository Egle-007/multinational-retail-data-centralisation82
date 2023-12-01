from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import numpy as np
from cleaning_functions import remove_non_numerics, invalid_numbers, phone_code, remove_alphabet, keep_alphabet, multiply_values
from sqlalchemy import create_engine




class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()        # Connection through engine
        self.extractor = DataExtractor()            # Returns dataframes 
        

    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector, 'legacy_users')          # Data to clean
        sorted_user_data = read_user_data.sort_values(by='index')                               # Sorts index into a sequential order

        # Removes meaningless info
        user_data = sorted_user_data.replace('NULL', np.nan)                                    # Relaces 'NULL' with np.nan. 
        user_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val. (nan val would go through the whole row - no useful info)
        user_data = user_data[user_data['country_code'].apply(lambda x: len(str(x)) <= 3)]      # Dropping other rows with meaningless info based on the expected length of a string
        
        user_data['country_code'] = user_data['country_code'].str.replace('GGB', 'GB')          # Converts country_code typo 'GGB' into 'GB'
        user_data['address'] = user_data['address'].str.replace('\n', ', ' )                    # Replaces '\n' from the data in the address column with a ','.

        # Assigns columns to an appropriate dTypes
        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string') 
        
        user_data['date_of_birth'] = user_data['date_of_birth'].apply(parse) 
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], infer_datetime_format=True, errors='coerse')
        
        user_data['join_date'] = user_data['join_date'].apply(parse) 
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], infer_datetime_format=True, errors='coerse')
            
        # Cleans phone numbers
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
        df.to_sql(table_name, engine_local, if_exists='replace')


    def clean_card_data(self):
        card_data = self.extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
       
        # Removes meaningless info                                                              # No duplicates were detected with .drop_duplicates
        card_data = card_data.replace('NULL', np.nan)                                           # Relaces 'NULL' with np.nan. 
        card_data.dropna(axis=0, inplace=True)                                                  # Drops rows with nan val.
        card_data = card_data[card_data['expiry_date'].apply(lambda x: len(str(x)) <= 5)]       # Dropping other rows with meaningless info based on the expected length of a string. Checked with .unique().
        
        card_data['card_number'] = card_data['card_number'].astype('string')                    # To remove '?' appearing in card numbers, first converts them to strings 
        card_data['card_number'] = card_data['card_number'].apply(remove_non_numerics)          # then applies remove_non_numerics. After using the function, dType turns back into object.
        
        # Assigns columns to an appropriate dTypes
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].apply(parse) 
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], infer_datetime_format=True, errors='coerse')
       
        card_data['expiry_date'] = '01/' + card_data['expiry_date'].astype(str)
        # card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], format='mixed')   # Because of further tasks had to 'hash' this bit
       
        # Sorts values
        card_data.sort_values(by='date_payment_confirmed', ascending = False, inplace = True)   # Sorts values based on the date when the paiment was confirmed in descending order
        card_data = card_data.reset_index(drop=True)                                            # Fixes index, was a repeat of 54.
       
        return card_data
    

    def called_clean_store_data(self):
        # stores_data = self.extractor.retrieve_stores_data()                                   

        stores_data = pd.read_csv('stores_data.csv')                                            # Due to status code 429: too often requests, extracted data^^ was exported as .csv^ and then the .csv file was used for cleaning.
        stores_data.set_index('index', inplace=True)
      
        # # Removes meaningless info                                                            # No duplicates were detected with .drop_duplicates                                                  
        stores_data = stores_data[stores_data['country_code'].apply(lambda x: len(str(x)) <= 4)]         # Returns only those rows that meets the condition. unique_country_codes = stores_data['country_code'].unique()   # print(unique_country_codes)
        stores_data.drop(['lat', 'Unnamed: 0'], axis=1, inplace=True)                           # Deletes empty/duplicate columns. Checked: unique_lat = stores_data['lat'].unique()       # print(unique_lat)
        stores_data.dropna(axis=0, how='all', inplace=True)                                     # Deletes rows with only the nan values.

        stores_data['address'] = stores_data['address'].str.replace('\n', ', ' )                # Replaces '\n' from the data in the address column with a ','.
        stores_data['continent'] = stores_data['continent'].str.replace('ee', '')               # Removes few 'ee' that were mixed into the continent names
        stores_data['staff_numbers'] = stores_data['staff_numbers'].apply(remove_non_numerics)  # Removes few letters mixed into numbers
        
        # Assigns columns to an appropriate dTypes
        stores_data['opening_date'] = stores_data['opening_date'].apply(parse) 
        stores_data['opening_date'] = pd.to_datetime(stores_data['opening_date'], infer_datetime_format=True, errors='coerse')

        stores_data['staff_numbers'] = stores_data['staff_numbers'].astype('int64')

        stores_data['latitude'] = stores_data['latitude'].astype('float64')
        stores_data['longitude'] = stores_data['longitude'].astype('float64')
      
        # Rearanges collumn order into more logical one
        column_names = ['store_type', 'store_code', 'opening_date', 'staff_numbers', 
                        'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude']
        stores_data = stores_data[column_names]
        print(stores_data.info())
        return stores_data
     

    def _convert_product_weights_(self):
        # products = self.extractor.extract_from_s3()
        products = pd.read_csv('products.csv')

        # Removes meaningless info that might affect weights conversion
        products = products[products['weight'].apply(lambda x: len(str(x)) < 10)]               # Removes 10 char long letter combinations, checked no other value len is >= 10
        products = products.replace('NULL', np.nan)                                             # Replaces 'null' with nan
        products.dropna(axis=0, inplace=True)                                                   # Removes rows with nan val

        # Prepares weights for conversion
        products['unit'] = products['weight'].apply(keep_alphabet)                              # Keeps only alphabetical characters in new col 'units'
        products['unit'] = products['unit'].str.replace('x', '')
        products['weight'] = products['weight'].apply(remove_alphabet)                          # Removes all characters except these: '0-9x.,'

        col = products.pop('unit')                                                              # Moves 'unit' column to a logical plase in the table.
        products.insert(4, col.name, col)

        # Multiplies product weights
        products['weight'] = products['weight'].apply(multiply_values)                          # Multiplies those values that had 'number x number' pattern
        products['weight'] = products['weight'].astype(float)
       
        # Converts weights
        conversion_factors = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.035274}                 # Sets up a dictionary of conversion factors
        products['weight'] = products['weight'].mul(products['unit'].map(conversion_factors))   # Maps converted weights
        products.loc[products['unit'].isin(conversion_factors), 'unit'] = 'kg'                  # Updates units
        
        return products
    

    def clean_products_data(self):
        products = self._convert_product_weights_()

        # Converts date to datetime dType
        products['date_added'] = pd.to_datetime(products['date_added'], format = 'mixed')

        # Cleans price and assigns to the right dType
        products['product_price'] = products['product_price'].str.replace('£', '').astype(float)    #  Removes '£' from the rows, so that values could be floats
      
        # Organises columns
        products['product_price_in_£'] = products['product_price']                              # Adds '£' sign to the column, to know the currency 
        products['weight_in_kg'] = products['weight']                                           # Adds unit to the name of the column, so that unit column could be dropped
        column_names = ['product_code', 'product_name', 'product_price_in_£',                   # Removes unnecessary col 'unit' and 'Unnamed: 0', that were not in the initial df
                        'weight_in_kg', 'category', 'EAN', 'uuid', 'date_added', 'removed']
        products = products[column_names]
        print(products)
        return products
 

    def clean_orders_data(self):
        orders_data = self.extractor.read_rds_table(self.connector, 'orders_table')
        
        # Organises columns
        column_names = ['date_uuid', 'user_uuid', 'card_number', 
                        'store_code', 'product_code', 'product_quantity']                       # Removing unnecessary columns, others seams fine with the right dTypes
        orders_data = orders_data[column_names]

        return orders_data
       

    def clean_date_details(self):
        date_details = pd.read_json('date_details.json')

        #Drops unnecessary rows
        date_details = date_details[date_details['day'].apply(lambda x: len(str(x)) <= 4)]      # Removes nonsense values
        date_details = date_details.replace('NULL', np.nan)                                     # Replaces 'NULL' with np.nan. 
        date_details.dropna(axis=0, inplace=True)
        date_details.drop_duplicates()
    
        return date_details



 
clean = DataCleaning()
# v = clean.clean_user_data()
# print(v)
# h = clean.clean_card_data()
# print(h)

# s = clean.called_clean_store_data()
# z = clean.upload_to_db(s, 'dim_store_details')
# x = clean.convert_product_weights()
# y = clean.clean_products_data()
# w = clean.clean_orders_data()
# u = clean.clean_date_details()
# z = clean.upload_to_db(h, 'dim_card_details') 