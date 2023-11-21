from database_utils import DatabaseConnector
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd

class DataExtractor:
         
    def read_rds_table(self, instance, table_name):
        engine = instance.init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col='index')  # index_col='index'
        df_copy = df.copy()
        return df_copy

con = DatabaseConnector()
extr = DataExtractor()
extr.read_rds_table(con, "legacy_users")



# ['first_name', 'last_name', 'date_of_birth', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'join_date', 'user_uuid']
