/* MRDC. Mile3. 
Task1. 
Changind data types of columns in orders_table. 
To use the max length of characters for dType  VARCHAR(?), firstly, the max length of those columns is checked. Then, dTypes are altered.*/

SELECT MAX(LENGTH(card_number)) 
FROM orders_table;	/* 19*/

SELECT MAX(LENGTH(store_code)) 
FROM orders_table;	/* 12*/

SELECT MAX(LENGTH(product_code)) 
FROM orders_table;	/* 11*/

ALTER TABLE orders_table 
     ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
	 ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
     ALTER COLUMN card_number TYPE VARCHAR(19),
	 ALTER COLUMN store_code TYPE VARCHAR(12),
	 ALTER COLUMN product_code TYPE VARCHAR(11),
	 ALTER COLUMN product_quantity TYPE smallint;

SELECT * FROM orders_table;


/* Task 2. 
Casting the columns of the dim_users table to correct dTypes */

SELECT MAX(LENGTH(country_code)) 
FROM dim_users;	/* 2 */

ALTER TABLE dim_users 
     ALTER COLUMN first_name TYPE VARCHAR(255),
	 ALTER COLUMN last_name TYPE VARCHAR(255),
	 ALTER COLUMN date_of_birth TYPE DATE,
	 ALTER COLUMN country_code TYPE VARCHAR(2),
     ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
	 ALTER COLUMN join_date TYPE DATE;
	 
SELECT * FROM dim_users;

/* Task 3. 
Casting the columns of the dim_store_details table to correct dTypes. 
The task requires merging of two columns however this has been cleared during cleaning process */

SELECT MAX(LENGTH(store_code)) 
FROM dim_store_details;	/* 11 */

SELECT MAX(LENGTH(country_code)) 
FROM dim_store_details;	/* 2 */

ALTER TABLE dim_store_details 
     ALTER COLUMN locality TYPE VARCHAR(255),
	 ALTER COLUMN store_type TYPE VARCHAR(255),
	 ALTER COLUMN longitude TYPE FLOAT,
	 ALTER COLUMN country_code TYPE VARCHAR(2),
	 ALTER COLUMN store_code TYPE VARCHAR(11),
	 ALTER COLUMN staff_numbers TYPE smallint,
	 ALTER COLUMN opening_date TYPE DATE,
	 ALTER COLUMN latitude TYPE FLOAT ,
	 ALTER COLUMN continent TYPE VARCHAR(255);
	 
SELECT * FROM dim_store_details;

/* Task 4. 
The task requires of removing '£' however this has been cleared during cleaning process. 
Using SQL it could be done like this: UPDATE dim_products SET product_price = REPLACE(product_price, '£', '').
Also, requires creating a new column with weight range of the products. */

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET 
   weight_class = 'Light'
WHERE 
   weight_in_kg < 2;
   
UPDATE dim_products
SET 
   weight_class = 'Mid_Sized'
WHERE 
   weight_in_kg BETWEEN 2 AND 40;
   
UPDATE dim_products
SET 
   weight_class = 'Heavy'
WHERE 
   weight_in_kg BETWEEN 40 AND 140;
   
UPDATE dim_products
SET 
   weight_class = 'Truck_required'
WHERE 
   weight_in_kg >= 140;

SELECT * FROM dim_products


/* Task 5.
Firstly, removed column was renamed to still_available, then dTypes changed. */

ALTER TABLE dim_products 
     RENAME removed TO still_available;

ALTER TABLE dim_products 
     ALTER COLUMN product_price_in_£ TYPE FLOAT,
	 ALTER COLUMN weight_in_kg TYPE FLOAT,
	 ALTER COLUMN "EAN" TYPE VARCHAR(17),
	 ALTER COLUMN product_code TYPE VARCHAR(11),
	 ALTER COLUMN date_added TYPE DATE,
	 ALTER COLUMN uuid SET DATA TYPE UUID USING uuid::UUID,
	 ALTER COLUMN weight_class TYPE VARCHAR(14),
	 ALTER COLUMN still_available TYPE BOOLEAN USING (still_available='Still_avaliable');

SELECT MAX(LENGTH("EAN")) 
FROM dim_products;	/* 17*/

SELECT MAX(LENGTH(product_code)) 
FROM dim_products;	/* 11*/

SELECT * FROM dim_products;


/* Task 6.
 dim_date_times dTypes changed. */
 
SELECT MAX(LENGTH(time_period)) 
FROM dim_date_times;	/* 10*/

 ALTER TABLE dim_date_times 
     ALTER COLUMN "month" TYPE VARCHAR(2),
	 ALTER COLUMN "year" TYPE VARCHAR(4),
	 ALTER COLUMN "day" TYPE VARCHAR(2),
	 ALTER COLUMN time_period TYPE VARCHAR(10),
	 ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID;
	 
SELECT * FROM dim_date_times;

/* Task 7.
 dim_card_details dTypes changed. */
 
SELECT MAX(LENGTH(expiry_date)) 
FROM dim_card_details;	/* 8*/

ALTER TABLE dim_card_details 
     ALTER COLUMN card_number TYPE VARCHAR(19),
	 ALTER COLUMN expiry_date TYPE VARCHAR(8),
	 ALTER COLUMN date_payment_confirmed TYPE DATE;
 
SELECT * FROM dim_card_details;



/* Task 8.
Create primary keys in all dim tables that match orders table.*/

SELECT * FROM orders_table;







