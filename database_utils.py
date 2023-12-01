import yaml
from sqlalchemy import create_engine, inspect
# import pandas as pd
# import psycopg2
# from  data_cleaning import DataCleaning
# import localdb_creds

class DatabaseConnector:                                                                                    # Class will be used to connect with a database.             
   
    def _read_db_creds_(self):                                                                              # read_db_creds method will read the credentials yaml file and return a dictionary of the credentials.
        with open('db_creds.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
            
        return db_creds

    
    def init_db_engine(self):                                                                               # init_db_engine method will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.
        db_creds = self._read_db_creds_()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@
                               {db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):                                                                       # list_db_tables will list table names             
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)     

           
connector = DatabaseConnector()

# ['legacy_store_details', 'legacy_users', 'orders_table']






