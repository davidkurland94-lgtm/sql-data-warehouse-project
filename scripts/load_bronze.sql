/*--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
This script creates the Stored Procedure to load the BRONZE layer of the Olist Dataset.
It includes auditing for row counts, execution time, and error handling for each table.

To execute: 
CALL bronze.load_bronze();
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql AS $$
DECLARE 
    v_start_ts TIMESTAMP;
    v_end_ts TIMESTAMP;
    v_row_count INT;
    v_error_msg TEXT;
BEGIN

    ----------------------------------------------------------------------------------------
    -- 1. ORDERS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.orders CASCADE;
        CREATE TABLE bronze.orders (
            order_id TEXT, customer_id TEXT, order_status TEXT,
            order_purchase_timestamp TEXT, order_approved_at TEXT,
            order_delivered_carrier_date TEXT, order_delivered_customer_date TEXT,
            order_estimated_delivery_date TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.orders (
            order_id, customer_id, order_status, order_purchase_timestamp, 
            order_approved_at, order_delivered_carrier_date, 
            order_delivered_customer_date, order_estimated_delivery_date
        )
        FROM '/csv_data/olist_orders_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'orders', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Orders logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'orders', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
        RAISE WARNING 'Orders Bronze Load Failed: %', v_error_msg;
    END;

    ----------------------------------------------------------------------------------------
    -- 2. PRODUCTS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.products CASCADE;
        CREATE TABLE bronze.products (
            product_id TEXT, product_category_name TEXT, product_name_lenght TEXT,
            product_description_lenght TEXT, product_photos_qty TEXT, product_weight_g TEXT,
            product_length_cm TEXT, product_height_cm TEXT, product_width_cm TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.products (
            product_id, product_category_name, product_name_lenght, 
            product_description_lenght, product_photos_qty, product_weight_g, 
            product_length_cm, product_height_cm, product_width_cm
        )
        FROM '/csv_data/olist_products_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'products', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Products logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'products', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
        RAISE WARNING 'Products Bronze Load Failed: %', v_error_msg;
    END;

    ----------------------------------------------------------------------------------------
    -- 3. CUSTOMERS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.customers CASCADE;
        CREATE TABLE bronze.customers (
            customer_id TEXT, customer_unique_id TEXT, customer_zip_code_prefix TEXT,
            customer_city TEXT, customer_state TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        FROM '/csv_data/olist_customers_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'customers', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Customers logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'customers', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
    END;

    ----------------------------------------------------------------------------------------
    -- 4. ORDER_ITEMS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.order_items CASCADE;
        CREATE TABLE bronze.order_items (
            order_id TEXT, order_item_id TEXT, product_id TEXT, seller_id TEXT,
            shipping_limit_date TEXT, price TEXT, freight_value TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
        FROM '/csv_data/olist_order_items_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'order_items', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Order Items logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'order_items', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
    END;

    ----------------------------------------------------------------------------------------
    -- 5. ORDER_PAYMENTS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.order_payment CASCADE;
        CREATE TABLE bronze.order_payment (
            order_id TEXT, payment_sequential TEXT, payment_type TEXT,
            payment_installments TEXT, payment_value TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.order_payment (order_id, payment_sequential, payment_type, payment_installments, payment_value)
        FROM '/csv_data/olist_order_payments_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'order_payment', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Order Payments logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'order_payment', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
    END;

    ----------------------------------------------------------------------------------------
    -- 6. SELLERS
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.sellers CASCADE;
        CREATE TABLE bronze.sellers (
            seller_id TEXT, seller_zip_code_prefix TEXT, seller_city TEXT, seller_state TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        FROM '/csv_data/olist_sellers_dataset.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'sellers', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Sellers logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'sellers', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
    END;

    ----------------------------------------------------------------------------------------
    -- 7. PRODUCT_CATEGORY
    ----------------------------------------------------------------------------------------
    BEGIN
        v_start_ts := clock_timestamp();
        
        DROP TABLE IF EXISTS bronze.product_category CASCADE;
        CREATE TABLE bronze.product_category (
            product_category_name TEXT, product_category_name_english TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        COPY bronze.product_category (product_category_name, product_category_name_english)
        FROM '/csv_data/product_category_name_translation.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_ts := clock_timestamp();

        INSERT INTO logging.load_stats (process_name, table_name, rows_inserted, start_ts, end_ts, duration_seconds, status)
        VALUES ('bronze.load_bronze', 'product_category', v_row_count, v_start_ts, v_end_ts, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)), 'SUCCESS');

        RAISE NOTICE 'Product Category logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status)
        VALUES ('bronze.load_bronze', 'product_category', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg);
    END;

END;
$$;
