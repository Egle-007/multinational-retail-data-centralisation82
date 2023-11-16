from database_utils import DatabaseConnector
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd

class DataExtractor:
         
    # This class will work as a utility class, in it you will be creating methods that help extract data from 
    # different data sources. The methods contained will be fit to extract data from a particular data source,
    # these sources will include CSV files, an API and an S3 bucket.
    # def read_rds_table(self, engine, table_name):
    #     if table_name not in DatabaseConnector.list_db_tables(self, engine):
    #         print(engine.list_db_tables(engine))
    # #add break when looping
    #     else:
    #         with engine.begin() as conn:
    #             df = pd.read_sql_table(table_name, con = conn, index_col='index')
    #             return df
    def read_rds_table(self, instance, table_name):
        engine = instance.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

con = DatabaseConnector()
extr = DataExtractor()
extr.read_rds_table(con, "legacy_users")




