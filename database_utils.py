import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    # Class will be used to connect with and upload data to the database. 
    # Created a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def __read_db_creds__(self):

        with open('db_creds.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def init_db_engine(self, db_creds):
    #     # Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise 
    #     # and return an sqlalchemy database engine.
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
    





