BEGIN; 

--Create the raw orders table

drop table if exists bronze.orders;

create table bronze.orders (
	order_id TEXT,
	customer_id TEXT,
	order_status TEXT,
	order_purchase_timestamp TEXT,
	order_approved_at TEXT,
	order_delivered_carrier_date TEXT,
	order_delivered_customer_date TEXT,
	order_estimated_delivery_date TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Orders data in

COPY bronze.orders (
    order_id, customer_id, order_status, order_purchase_timestamp, 
    order_approved_at, order_delivered_carrier_date, 
    order_delivered_customer_date, order_estimated_delivery_date
)
FROM '/csv_data/olist_orders_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw products table

drop table if exists bronze.products;

create table bronze.products (
	product_id TEXT,
	product_category_name TEXT,
	product_name_lenght TEXT,
	product_description_lenght TEXT,
	product_photos_qty TEXT,
	product_weight_g TEXT,
	product_length_cm TEXT,
	product_height_cm TEXT,
	product_width_cm TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Products data in

COPY bronze.products (
    product_id, product_category_name, product_name_lenght, 
    product_description_lenght, product_photos_qty, product_weight_g, 
    product_length_cm, product_height_cm, product_width_cm
)
FROM '/csv_data/olist_products_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw customers table

drop table if exists bronze.customers;

create table bronze.customers (
	customer_id TEXT,
	customer_unique_id TEXT,
	customer_zip_code_prefix TEXT,
	customer_city TEXT,
	customer_state TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Customers data in

COPY bronze.customers (
    customer_id, customer_unique_id, customer_zip_code_prefix, 
    customer_city, customer_state
)
FROM '/csv_data/olist_customers_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw geolocation table

drop table if exists bronze.geolocation;

create table bronze.geolocation (
	geolocation_zip_code_prefix TEXT,
	geolocation_lat TEXT,
	geolocation_lng TEXT,
	geolocation_city TEXT,
	geolocation_state TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 
-- Copy Geolocation data in

COPY bronze.geolocation (
    geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, 
    geolocation_city, geolocation_state
)
FROM '/csv_data/olist_geolocation_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw order_items table

drop table if exists bronze.order_items;

create table bronze.order_items (
	order_id TEXT,
	order_item_id TEXT,
	product_id TEXT,
	seller_id TEXT,
	shipping_limit_date TEXT,
	price TEXT,
	freight_value TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Order Items data in

COPY bronze.order_items (
    order_id, order_item_id, product_id, seller_id, 
    shipping_limit_date, price, freight_value
)
FROM '/csv_data/olist_order_items_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw order_payment table

drop table if exists bronze.order_payment;

create table bronze.order_payment (
	order_id TEXT,
	payment_sequential TEXT,
	payment_type TEXT,
	payment_installments TEXT,
	payment_value TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Order Payment data in

COPY bronze.order_payment (
    order_id, payment_sequential, payment_type, 
    payment_installments, payment_value
)
FROM '/csv_data/olist_order_payments_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw order_reviews table

drop table if exists bronze.order_reviews;

create table bronze.order_reviews (
	review_id TEXT,
	order_id TEXT,
	review_score TEXT,
	review_comment_title TEXT,
	review_comment_message TEXT,
	review_creation_date TEXT,
	review_answer_timestamp TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Order Reviews data in

COPY bronze.order_reviews (
    review_id, order_id, review_score, review_comment_title, 
    review_comment_message, review_creation_date, review_answer_timestamp
)
FROM '/csv_data/olist_order_reviews_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw order_sellers table

drop table if exists bronze.sellers;

create table bronze.sellers (
	seller_id TEXT,
	seller_zip_code_prefix TEXT,
	seller_city TEXT,
	seller_state TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Sellers data in
COPY bronze.sellers (
    seller_id, seller_zip_code_prefix, seller_city, seller_state
)
FROM '/csv_data/olist_sellers_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

--Create the raw product_category table

drop table if exists bronze.product_category;

create table bronze.product_category (
	product_category_name TEXT,
	product_category_name_english TEXT,
	ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy Product Category Translation data in

COPY bronze.product_category (
    product_category_name, product_category_name_english
)
FROM '/csv_data/product_category_name_translation.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

DO $$
BEGIN
	RAISE NOTICE 'Bronze Layer Load Complete at %',NOW();
END
$$;

COMMIT;


