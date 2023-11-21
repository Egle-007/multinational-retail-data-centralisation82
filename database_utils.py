import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg2
# from  data_cleaning import DataCleaning
import localdb_creds

class DatabaseConnector:
   
    # Class will be used to connect with and upload data to the database. 
    # Created a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
        return db_creds

  # Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.  
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)     

    # def upload_to_db(self, df, table_name):
    #     # user_data = self.cleaner.clean_user_data()
    #     engine_local = create_engine(f"{localdb_creds['DATABASE_TYPE']}+{localdb_creds['DBAPI']}://{localdb_creds['USER']}:{localdb_creds['PASSWORD']}@{localdb_creds['HOST']}:{localdb_creds['PORT']}/{localdb_creds['DATABASE']}")
        
    #     df.to_sql(table_name, engine_local)

        
connector = DatabaseConnector()



# ['legacy_store_details', 'legacy_users', 'orders_table']






