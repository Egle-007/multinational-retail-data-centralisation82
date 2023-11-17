# import all classes

import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
# from database_utils import DatabaseConnector
import psycopg2


class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
        return db_creds

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)


class DataExtractor:
    def read_rds_table(self, instance, table_name):
        engine = instance.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

con = DatabaseConnector()
extr = DataExtractor()
extr.read_rds_table(con, "legacy_users")

class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()
        self.extractor = DataExtractor()

    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table(self.connector,'legacy_users')
        return read_user_data

clean = DataCleaning()
cl = clean.clean_user_data()
print(cl)