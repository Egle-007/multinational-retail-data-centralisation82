# multinational-retail-data-centralisation82

## About this project

A multinational company that sells various goods across the globe has their sales data spread across various data sources. The members of the team has no easy access to the data therefore struggle to produce appropriate analysis.


### Purpose

To make the company more data-driven so that it's up-to-date sales data would be accessible to the staff from one centralised location, therefore improve business decision making.

### Development goals

1. Produce a system that extracts, cleans and stores company's sales data in one database so that it is accessed from one centralised location and acts as a single source of truth for sales data.

2. Query the database to get up-to-date metrics for the business and answer business questions so tat the company could then make more data-driven decisions and get a better understanding of it's sales. 

## Main tools 

    1. VSCode 
    2. Python
    3. SQL
    4. pgAdmin
    5. pandas 
    6. numpy 
    7. sqlalchemy
    8. tabula 
    9. requests 
    10. boto3
    11. botocore
    12. json
    13. yaml


### Classes and Methods


There were three classes built to extract, transform and load data to the local database. Each class had several methods to fulfill their purposes.

**DatabaseConnector** class is used for connection to AWS RDS  database where part of the sales data was kept prior to the project. **Methods** used in the class:

* **read_db_creds** - reads database credentials and returns it in a way that it could be utilised by another method. 
* **init_db_engine** - uses the credentials from *read_db_creds* to connect to RDS via sqlalchemy database engine.
* **list_db_tables** - uses the previously mentioned engine and lists database table names.

**DataExtractor** class is used for data extraction from various data sources. **Methods** used in the class:

* **read_rds_table** - creates the instance of the engine to connect to RDS via DataConnector and returns a pandas dataframe of a chosen table.
* **retrieve_pdf_data** - extracts data from a .pdf file and adds all pages into one pandas dataframe.
* **list_number_of_stores** - retrieves data from an API and returns the number of the company's stores.
* **retrieve_stores_data** - gathers all data from the sores together and returns a pandas dataframe.
* **extract_from_s3** - downloads data from the S3

**DataCleaning** class is for cleaning of the data and uploading to a centralised location. **Methods** used in the class:

* **clean_user_data** - cleans user data.
* **clean_card_data** - cleans card data.
* **called_clean_store_data** - cleans store data.
* **convert_product_weights** - converts products weights into single type of unit - kg.
* **clean_products_data** - cleans product data.
* **clean_orders_data** - cleans orders data.
* **clean_date_details** - cleans and organises date details.
* **upload_to_db** - uploads dataframes into the local database.


## Cloning to your local device

You are able to clone the repository by typing the following command in the command line:

    git clone -c https://github.com/Egle-007/multinational-retail-data-centralisation82.git

## Other

### Main files

    1. Milestone 1 and 2: 
        - database_utils.py - connecting to databases;
        - data_extraction.py - extraction of the data;
        - data_cleaning.py - data cleaning and uploading to local database.
        - cleaning_functions.py - functions used in data cleaning process.
    2. Milestone 3 and 4:
        - MRDC_mile3.sql - SQL tables alterations. Creates database schema.
        - MRDC_mile.sql - SQL queries.
        - SQL_result_visual.ipynb - visual presentation of SQL table alterations and queries.

### Lisence

    This project is licensed under the MIT lisence.




