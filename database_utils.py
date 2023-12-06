import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:                                                                                   
    ''' 
    DatabaseConnector is a class used to connect to remote database (db).
    -------
    Methods:
    -------
    _read_db_creds_(self)
        Read the credentials in yaml file and return a dictionary of the credentials.
    init_db_engine(self)
        Read the credentials from the return of read_db_creds and initialise (use the credentials to log into the db) and return an sqlalchemy database engine.
    list_db_tables(self, engine)
        Print table names.
    '''


    def _read_db_creds_(self):                          # protected method not to be included in the autocompletion hints        
        with open('db_creds.yaml', 'r') as f:           # takes a .yaml file in reading mode and 
            db_creds = yaml.safe_load(f)                # uses the safe_load method from the yaml package to read data from a YAML file 
            
        return db_creds

    
    def init_db_engine(self):                           # method to be used for connection to the AWS RDS
        db_creds = self._read_db_creds_()               # uses previous method to access the credentials for not having to write the credentials directly into the code
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine                                   

    def list_db_tables(self, engine):                                
        inspector = inspect(engine)                     # uses inspect() function to get access to table names in the db
        for table in inspector.get_table_names():
            print(table)     

           
connector = DatabaseConnector()








