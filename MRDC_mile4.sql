-- MRDC. Mile 4. 
-- Task 1. 
-- Quering data regarding which countries company currently operates in and which country now has the most stores. 
-- It includes web store as it is located in GB, therefore it is 266 instead of 265. 

SELECT 
      country_code AS country,
      COUNT(store_code) AS total_no_stores
FROM 
      dim_store_details
GROUP BY
      country
ORDER BY
      total_no_stores DESC;


-- Task 2.
-- Which locations currecntly have the most stores?
-- To look exactly like in the task, added 'LIMIT 7'.

SELECT 
      locality,
	  COUNT(store_code) AS total_no_stores
FROM
      dim_store_details
GROUP BY
      locality
ORDER BY
      total_no_stores DESC
LIMIT 
      7;


-- Task 3. 
-- Months of most sales.

SELECT ROUND(CAST(SUM(product_price_in_£ * product_quantity) AS NUMERIC), 2) AS total_sales,   -- summerising all sold products multiplied by their price
       month
FROM orders_table
JOIN
     dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid     -- Joining dim_date_times with orders_table via date_uuid
JOIN 
     dim_products ON dim_products.product_code = orders_table.product_code   -- joining dim_products with orders_table via product_code
GROUP BY 
     month
ORDER BY 
     total_sales DESC;
-- Could add limit 6 to have exactly as in the task. 


-- Task 4.
-- Sales online vs offline.
-- Using CASE/END for this task. Orders table shows all orders, so each line is a new order (=sale) therefore index col used as a number_of_sales.
	
SELECT 
	  COUNT(index) AS number_of_sales,
	  SUM(product_quantity) AS product_quantity_count,
CASE
      WHEN store_code LIKE 'WEB%' THEN 'Web'     -- if there is 'WEB%' in the store code it will mark it as 'Web' in a new col, else 'Offline'
      ELSE 'Offline'
END AS location    -- new col would be named 'location'
From 
      orders_table
GROUP BY 
      location 
ORDER BY 
      location DESC;


-- Task 5.
-- Total and percentage of sales coming from each of the different store types.

SELECT store_type,
       ROUND((SUM(product_price_in_£ * product_quantity)::NUMERIC), 2) AS total_sales,
	  ROUND(SUM(product_price_in_£ * product_quantity)::NUMERIC/
			 SUM(SUM(product_price_in_£ * product_quantity):: NUMERIC) over() * 100, 2) 
			 AS "percentage_total(%)" -- SUM(SUM(total_sales)) covers all locations, SUM(total_sales) covers just one location.
FROM 
     orders_table
JOIN
     dim_store_details ON dim_store_details.store_code = orders_table.store_code    --dim_store_details joined with orders_table via store_code
JOIN 
     dim_products ON dim_products.product_code = orders_table.product_code          --dim_products joined with orders_table via product_code
GROUP BY 
     store_type
Order BY 
     "percentage_total(%)" DESC;
	 


-- Task 6.
-- Which months in which years have had the most sales historically.

SELECT ROUND((SUM(product_price_in_£ * product_quantity)::NUMERIC), 2) AS total_sales,
       year,
       month
FROM 
     orders_table
JOIN
     dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid            -- dim_date_times joined with orders_table via date_uuid
JOIN
	 dim_products ON dim_products.product_code = orders_table.product_code          -- dim_products joined with orders_table via product_code
GROUP BY 
     year, month
Order BY 
     total_sales DESC
LIMIT 10;


-- Task 7.
-- Overall staff numbers by country.

SELECT 
      SUM(staff_numbers) AS total_staff_numbers,  -- sum up all staff and group them by country.
	  country_code AS country
FROM 
      dim_store_details
GROUP BY
      country
ORDER BY
      total_staff_numbers DESC;


-- Task 8.
-- Sales in German stores. Sum up all sales and filter where the country code is 'DE'

SELECT ROUND((SUM(product_price_in_£ * product_quantity)::NUMERIC), 2) AS total_sales,
       store_type,
       dim_store_details.country_code    
FROM 
     orders_table
JOIN
     dim_store_details ON dim_store_details.store_code = orders_table.store_code    --dim_store_details joined with orders_table via store_code
JOIN 
     dim_products ON dim_products.product_code = orders_table.product_code          --dim_products joined with orders_table via product_code
WHERE 
     country_code LIKE 'DE'
GROUP BY 
     store_type, country_code 
ORDER BY 
     total_sales;
	
	
	
-- Task 9.
-- The average time taken between each sale grouped by year. 
-- Building two CTE's where one gives initial time, another gives "lead" time (the next value in a col). 
-- Then calculating average of their subtraction, which gives an interval of time between two sales.

WITH CTE_initial AS (
	SELECT
	      (year || '-' || month || '-' || day || ' ' || timestamp)::TIMESTAMP AS initial_time,   -- concatenating a full datetime
	      year 
	FROM dim_date_times
	ORDER BY initial_time DESC
), time_interval AS (
	SELECT year, 
	       initial_time,
	       LEAD(initial_time, 1) OVER(ORDER BY initial_time DESC) AS lead_time
	FROM CTE_initial
)   SELECT year,
           AVG(initial_time - lead_time) AS avg_time
	FROM time_interval
	GROUP BY year
	order by avg_time desc
	







