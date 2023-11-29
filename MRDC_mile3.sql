/* MRDC. Mile3. Task1. 
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