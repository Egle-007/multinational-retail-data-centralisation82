import yaml

class DatabaseConnector:
    # Class will be used to connect with and upload data to the database. 
    # Created a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self):

        with open('db_creds.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
        print(db_creds)

connector = DatabaseConnector()
connector.read_db_creds()

