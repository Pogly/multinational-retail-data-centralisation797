ALTER TABLE orders_table
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(20),
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(11),
    ALTER COLUMN product_quantity SET DATA TYPE SMALLINT,
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
    ALTER COLUMN first_name SET DATA TYPE VARCHAR(255),
    ALTER COLUMN last_name SET DATA TYPE VARCHAR(255),
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(3),
    ALTER COLUMN join_date SET DATA TYPE DATE,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_store_details
    ALTER COLUMN locality SET DATA TYPE VARCHAR(255),
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT,
    ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
    ALTER COLUMN opening_date SET DATA TYPE DATE;

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');
COMMIT;

ALTER TABLE dim_products 
ADD COLUMN weight_class TEXT;

UPDATE dim_products
SET weight_class = 
    CASE
        WHEN weight >= 140 THEN 'Truck_Required'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight < 2 THEN 'Light'
    END;


ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC,
    ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC,
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(12),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID, 
    ALTER COLUMN weight_class SET DATA TYPE VARCHAR(16);

ALTER TABLE dim_date_times
    ALTER COLUMN month SET DATA TYPE VARCHAR(2),
    ALTER COLUMN year SET DATA TYPE VARCHAR(4),
    ALTER COLUMN day SET DATA TYPE VARCHAR(2),
    ALTER COLUMN timestamp  TYPE timestamp USING timestamp::timestamp,
    ALTER COLUMN time_period SET DATA TYPE VARCHAR(15),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE dim_card_details
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(30),
    ALTER COLUMN expiry_date SET DATA TYPE VARCHAR(8),
    ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE;


ALTER TABLE dim_users 
ADD CONSTRAINT pk_users PRIMARY KEY (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE dim_card_details 
ADD CONSTRAINT pk_card_details  PRIMARY KEY (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_details FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE dim_products
ADD CONSTRAINT pk_products PRIMARY KEY (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_products FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_details PRIMARY KEY (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_details FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);


SELECT country_code, COUNT(country_code)
FROM dim_store_details
GROUP BY country_code

SELECT locality, 
	COUNT(locality) as Store_count
FROM dim_store_details

GROUP BY locality
ORDER BY Store_count DESC;

SELECT dim_date_times.month, 
SUM(dim_products.product_price * product_quantity)AS total_price
FROM orders_table

JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code

GROUP BY dim_date_times.month
ORDER BY total_price DESC

SELECT
    CASE 
        WHEN store_code = 'WEB-1388012W' THEN 'Web' 
        ELSE 'Offline' 
    END AS sales_channel,                                   
    SUM(product_quantity) AS Product_Amount,
    COUNT(store_code) AS Number_Of_Sales
FROM 
    orders_table

GROUP BY 
    sales_channel;


SELECT DISTINCT dim_store_details.store_type AS stores,
SUM(dim_products.product_price * product_quantity)AS Total_Sales,
SUM(dim_products.product_price * orders_table.product_quantity) * 100.0 
/ SUM(SUM(dim_products.product_price * orders_table.product_quantity)) OVER () AS percentage_of_total
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY stores;


SELECT 
dim_date_times.year,
dim_date_times.month,
SUM(dim_products.product_price * product_quantity)AS Total_Sales
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year,dim_date_times.month
ORDER BY Total_Sales DESC


SELECT
dim_store_details.country_code,
COUNT(dim_store_details.staff_numbers)AS total_staff_numbers
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.country_code


SELECT
dim_store_details.country_code,
dim_store_details.store_type,
SUM(dim_products.product_price * product_quantity)AS Total_Sales
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY Total_Sales DESC


SELECT 
    dim_date_times.year,
    dim_date_times.month,
    dim_date_times.day,
    dim_date_times.timestamp,
    dim_date_times.timestamp = LAG(dim_date_times.timestamp) OVER (ORDER BY dim_date_times.year DESC, dim_date_times.month DESC, dim_date_times.day DESC, dim_date_times.timestamp DESC) AS time_difference
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
ORDER BY 
    dim_date_times.year ASC, dim_date_times.month ASC, dim_date_times.day ASC, dim_date_times.timestamp ASC;


WITH gen_tIMESTAMP AS (SELECT 
	dim_date_times.year,
    TO_TIMESTAMP(
        CAST(dim_date_times.year AS TEXT) || '-' || 
        CAST(dim_date_times.month AS TEXT) || '-' || 
        CAST(dim_date_times.day AS TEXT) || ' ' || 
        (dim_date_times.timestamp),
        'YYYY-MM-DD HH24:MI:SS'
    ) AS full_timestamp  
FROM
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	),
calac_lead as(
	SELECT LEAD(full_timestamp,-1) OVER( ORDER BY full_timestamp) AS lead_timestamp,
	full_timestamp,
	gen_tIMESTAMP.year
	FROM gen_tIMESTAMP
	)
SELECT
calac_lead.year,
AVG(full_timestamp - lead_timestamp) AS actual_time_taken
FROM 
    calac_lead
GROUP BY
   	calac_lead.year
ORDER BY
	actual_time_taken DESC