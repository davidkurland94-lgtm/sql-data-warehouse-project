/*--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
This is the script to load the silver layer of the Olist Commerce Dataset. 
Data transformations, derived columns are included.

In order to run this procedure, run 'CALL silver.load_silver();'
*/
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
create or replace procedure silver.load_silver() language plpgsql as $$
declare 
	v_start_ts TIMESTAMP;
    v_end_ts TIMESTAMP;
    v_row_count INT;
	v_error_msg TEXT;
    v_error_detail TEXT;
begin
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for ORDERS (Wrapped in a sub-block)
	BEGIN

	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.ORDERS CASCADE;
	
	CREATE TABLE SILVER.ORDERS AS
	SELECT
		ORDER_ID,
		CUSTOMER_ID,
		ORDER_STATUS,
		CASE 
	        WHEN order_purchase_timestamp::TIMESTAMP > COALESCE(order_delivered_carrier_date::TIMESTAMP, ORDER_APPROVED_AT::TIMESTAMP) 
	        THEN COALESCE(order_delivered_carrier_date::TIMESTAMP, ORDER_APPROVED_AT::TIMESTAMP) 
	        ELSE order_purchase_timestamp::TIMESTAMP 
   		END AS purchase_ts, --Swapping original purchase_ts data with delivered_carrier_ts data when purchase date is more recent than delivered to carrier date
   		CASE 
	        WHEN order_purchase_timestamp::TIMESTAMP > COALESCE(order_delivered_carrier_date::TIMESTAMP, ORDER_APPROVED_AT::TIMESTAMP) 
	        THEN order_purchase_timestamp::TIMESTAMP 
	        ELSE COALESCE(order_delivered_carrier_date::TIMESTAMP, ORDER_APPROVED_AT::TIMESTAMP) 
			--Imputing order_approved_at 
			--into places where delivered carrier date is NULL but the delivery is marked as 'delivered'
    	END AS delivered_carrier_ts,
		CASE 
		    WHEN order_delivered_carrier_date IS NULL AND order_status = 'delivered' THEN 'Imputed Carrier Date'
		    WHEN order_purchase_timestamp::TIMESTAMP > order_delivered_carrier_date::TIMESTAMP THEN 'Swapped Dates'
		    ELSE 'Valid'
		END AS logistics_quality_flag, --Documenting records that date fields were swapped or imputed due to missing data
		ORDER_APPROVED_AT::TIMESTAMP AS APPROVAL_TS,
		ORDER_DELIVERED_CUSTOMER_DATE::TIMESTAMP AS DELIVERED_CUSTOMER_TS,
		ORDER_ESTIMATED_DELIVERY_DATE::TIMESTAMP AS ESTIMATED_TS,
		EXTRACT(DAY FROM(ORDER_DELIVERED_CARRIER_DATE::TIMESTAMP - ORDER_PURCHASE_TIMESTAMP::TIMESTAMPTZ)
		) AS SELLER_TO_CARRIER_LEAD_TIME,-- minus value means delivered earlier than expected
		EXTRACT(
			DAY
			FROM
				(ORDER_DELIVERED_CUSTOMER_DATE::TIMESTAMPTZ - ORDER_ESTIMATED_DELIVERY_DATE::TIMESTAMPTZ)
		) AS DELAY_DAYS,
		CASE
			WHEN ORDER_DELIVERED_CUSTOMER_DATE IS NOT NULL THEN EXTRACT(
				DAY
				FROM
					(ORDER_DELIVERED_CUSTOMER_DATE::TIMESTAMPTZ - ORDER_PURCHASE_TIMESTAMP::TIMESTAMPTZ)
			)
			ELSE NULL --NULL in order to not affect averages
		END AS LEAD_TIME_DAYS,
		CASE
			WHEN ORDER_DELIVERED_CUSTOMER_DATE IS NULL THEN 0
			WHEN ORDER_DELIVERED_CUSTOMER_DATE::DATE > ORDER_ESTIMATED_DELIVERY_DATE::DATE THEN 1
			ELSE 0
		END AS IS_LATE_INT,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.ORDERS;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Add constraints and indexes to orders table
	ALTER TABLE SILVER.ORDERS
	ADD PRIMARY KEY (ORDER_ID);
	CREATE INDEX IDX_ORDERS_CUSTOMER_ID ON SILVER.ORDERS (CUSTOMER_ID);
	CREATE INDEX IDX_ORDERS_PURCHASE_TS ON SILVER.ORDERS (PURCHASE_TS);
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'orders',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)) --Calculate total seconds of query
	    );
  
  --Make sure VALUES and WARNING and RAISE NOTICE has proper table name!
	
	    RAISE NOTICE 'Orders logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'orders', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Orders Load Failed: %', v_error_msg;

	END;
	----------------------------------------------------------------------------------------	
	----------------------------------------------------------------------------------------
	--DDL + Load for ORDER_ITEMS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.ORDER_ITEMS CASCADE;
	
	CREATE TABLE SILVER.ORDER_ITEMS AS
	SELECT
		ORDER_ID,
		ORDER_ITEM_ID::INT AS ITEM_SEQ,
		PRODUCT_ID,
		SELLER_ID,
		PRICE::DECIMAL(10, 2) AS ITEM_PRICE,
		FREIGHT_VALUE::DECIMAL(10, 2) AS FREIGHT_PRICE,
		(PRICE::DECIMAL + FREIGHT_VALUE::DECIMAL) AS TOTAL_ITEM_VALUE,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.ORDER_ITEMS;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'order_items', --Manually needs to be updated
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
	    RAISE NOTICE 'Order_items logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'order_items', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        --Make sure VALUES and WARNING and RAISE NOTICE has proper table name!
		
        RAISE WARNING 'Order_items Load Failed: %', v_error_msg;
	END;	
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for ORDER_PAYMENTS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.ORDER_PAYMENTS CASCADE;
	
	CREATE TABLE SILVER.ORDER_PAYMENTS AS
	SELECT
		ORDER_ID,
		PAYMENT_SEQUENTIAL::INT AS PAYMENT_SEQ,
		PAYMENT_TYPE,
		PAYMENT_INSTALLMENTS::INT AS INSTALLMENTS,
		PAYMENT_VALUE::DECIMAL(10, 2) AS PAYMENT_VALUE,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.ORDER_PAYMENT;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'order_payments',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
	    RAISE NOTICE 'Order_payments logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'order_payments', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Order_payments Load Failed: %', v_error_msg;
	END;
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for CUSTOMERS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.CUSTOMERS CASCADE;
	
	CREATE TABLE SILVER.CUSTOMERS AS
	SELECT
		CUSTOMER_ID,
		CUSTOMER_UNIQUE_ID,
		TRIM(CUSTOMER_ZIP_CODE_PREFIX) AS CUSTOMER_ZIP,
		UPPER(TRIM(CUSTOMER_CITY)) AS CUSTOMER_CITY, --Normalizing city name for easier matching
		UPPER(TRIM(CUSTOMER_STATE::VARCHAR(2))) AS CUSTOMER_STATE, --Adding character constraint
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.CUSTOMERS;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Add constraints and indexes
	ALTER TABLE SILVER.CUSTOMERS
	ADD PRIMARY KEY (CUSTOMER_ID);
	CREATE INDEX IDX_SILVER_CUSTOMERS_UNIQUE_ID ON SILVER.CUSTOMERS (CUSTOMER_UNIQUE_ID);
	CREATE INDEX IDX_SILVER_CUSTOMERS_CITY ON SILVER.CUSTOMERS (CUSTOMER_CITY);
	CREATE INDEX IDX_SILVER_CUSTOMERS_ZIP ON SILVER.CUSTOMERS (CUSTOMER_ZIP);


	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'customers',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );

	    RAISE NOTICE 'Customers logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'customers', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Customers Load Failed: %', v_error_msg;
	END;
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for ORDER_REVIEWS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.ORDER_REVIEWS CASCADE;
	
	CREATE TABLE SILVER.ORDER_REVIEWS AS
	SELECT
		REVIEW_ID,
		ORDER_ID,
		REVIEW_SCORE::INT AS REVIEW_SCORE,
		TRIM(REVIEW_COMMENT_TITLE) AS REVIEW_TITLE,
		TRIM(REVIEW_COMMENT_MESSAGE) AS REVIEW_MESSAGE,
		REVIEW_CREATION_DATE::TIMESTAMP AS SURVEY_SENT_TS,
		REVIEW_ANSWER_TIMESTAMP::TIMESTAMP AS SURVEY_ANSWERED_TS,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.ORDER_REVIEWS;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'order_reviews',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
	    RAISE NOTICE 'Order_reviews logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'order_reviews', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Order_reviews Load Failed: %', v_error_msg;
	END;	
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for PRODUCT_CATEGORY
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.PRODUCT_CATEGORY CASCADE;
	
	CREATE TABLE SILVER.PRODUCT_CATEGORY AS
	SELECT
		UPPER(TRIM(PRODUCT_CATEGORY_NAME)) AS PRODUCT_CATEGORY_NAME_PG,
		UPPER(TRIM(PRODUCT_CATEGORY_NAME_ENGLISH)) AS PRODUCT_CATEGORY_NAME_EN,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.PRODUCT_CATEGORY;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'product_category',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
	    RAISE NOTICE 'Product_category logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'product_category', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Product_category Load Failed: %', v_error_msg;
	END;	
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for PRODUCTS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.PRODUCTS CASCADE;
	
	CREATE TABLE SILVER.PRODUCTS AS
	SELECT
		PRODUCT_ID,
		TRIM(UPPER(PC.product_category_name_ENGLISH)) AS product_category,
		PRODUCT_NAME_LENGHT::INT AS PRODUCT_NAME_LENGTH,
		PRODUCT_DESCRIPTION_LENGHT::INT AS PRODUCT_DESCRIPTION_LENGTH,
		PRODUCT_PHOTOS_QTY::INT AS PRODUCT_PHOTOS_QTY,
		PRODUCT_WEIGHT_G::DECIMAL AS PRODUCT_WEIGHT_G,
		PRODUCT_LENGTH_CM::DECIMAL AS PRODUCT_LENGTH_CM,
		PRODUCT_HEIGHT_CM::DECIMAL AS PRODUCT_HEIGHT_CM,
		PRODUCT_WIDTH_CM::DECIMAL AS PRODUCT_WIDTH_CM,
		CASE
			WHEN PRODUCT_LENGTH_CM::DECIMAL IS NOT NULL
			AND PRODUCT_HEIGHT_CM::DECIMAL IS NOT NULL
			AND PRODUCT_WIDTH_CM::DECIMAL IS NOT NULL THEN (
				PRODUCT_LENGTH_CM::DECIMAL * PRODUCT_HEIGHT_CM::DECIMAL * PRODUCT_WIDTH_CM::DECIMAL
			)
			ELSE NULL
		END AS VOLUME_CM3, --Created volume of product to possibly optimize space capabilities for carriers
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.PRODUCTS P
		LEFT JOIN BRONZE.PRODUCT_CATEGORY PC ON PC.product_category_name = P.product_category_name;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	ALTER TABLE SILVER.PRODUCTS
	ADD PRIMARY KEY (PRODUCT_ID);
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'products',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
	    RAISE NOTICE 'Products logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'products', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Products Load Failed: %', v_error_msg;	
	END;
	----------------------------------------------------------------------------------------
	--DDL + Load for SELLERS
	BEGIN
	v_start_ts := clock_timestamp();
	
	DROP TABLE IF EXISTS SILVER.SELLERS CASCADE;
	
	CREATE TABLE SILVER.SELLERS AS
	SELECT
		SELLER_ID,
		TRIM(SELLER_ZIP_CODE_PREFIX) AS SELLER_ZIP,
		UPPER(TRIM(SELLER_CITY)) AS SELLER_CITY,
		UPPER(TRIM(SELLER_STATE::VARCHAR(2))) AS SELLER_STATE,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.SELLERS;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();
	
	
	ALTER TABLE SILVER.SELLERS
	ADD PRIMARY KEY (SELLER_ID);

	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'sellers',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	

	    RAISE NOTICE 'Sellers logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'sellers', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Sellers Load Failed: %', v_error_msg;	
	END;
	----------------------------------------------------------------------------------------
	----------------------------------------------------------------------------------------
	--DDL + Load for GEOLOCATION
	BEGIN
	v_start_ts := clock_timestamp();

	DROP TABLE IF EXISTS SILVER.GEOLOCATION CASCADE;
	
	CREATE TABLE SILVER.GEOLOCATION AS
	SELECT DISTINCT ON (geolocation_zip) -- Deduplicate the Zips in the load
		TRIM(GEOLOCATION_ZIP_CODE_PREFIX) AS GEOLOCATION_ZIP,
		GEOLOCATION_LAT::DECIMAL AS GEOLOCATION_LAT,
		GEOLOCATION_LNG::DECIMAL AS GEOLOCATION_LNG,
		UPPER(TRIM(GEOLOCATION_CITY)) AS GEOLOCATION_CITY,
		UPPER(TRIM(GEOLOCATION_STATE::VARCHAR(2))) AS GEOLOCATION_STATE,
		CURRENT_TIMESTAMP AS SILVER_INGESTION_TS
	FROM
		BRONZE.GEOLOCATION
	ORDER BY geolocation_zip, geolocation_lat;

	-- Capture how many rows were actually created
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_end_ts := clock_timestamp();

	-- Create index for geolocation_zip to improve join performance in views

	CREATE INDEX IDX_SILVER_GEOLOCATION_ZIP ON SILVER.GEOLOCATION (GEOLOCATION_ZIP);
	
	-- Insert metrics into new logging table
	INSERT INTO logging.load_stats (
	        process_name, 
	        table_name, 
	        rows_inserted, 
	        start_ts, 
	        end_ts, 
	        duration_seconds
	    )
	    VALUES (
	        'silver.load_silver',
	        'geolocation',
	        v_row_count,
	        v_start_ts,
	        v_end_ts,
	        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts))
	    );
	
 		RAISE NOTICE 'Geolocation logged: % rows in % seconds', v_row_count, EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
		EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (process_name, table_name, start_ts, end_ts, status, executed_by)
        VALUES ('silver.load_silver', 'geolocation', v_start_ts, clock_timestamp(), 'FAILED: ' || v_error_msg, CURRENT_USER);
        
        RAISE WARNING 'Geolocation Load Failed: %', v_error_msg;
	END;
COMMIT;

END;
$$
