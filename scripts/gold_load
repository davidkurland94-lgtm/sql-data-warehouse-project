--# PostgreSQL Gold Layer Stored Procedure (Star Schema)

/*--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
This script creates the Stored Procedure to load the GOLD layer of the Olist Dataset
using a STAR SCHEMA design.

It replaces the logistics and sales materialized views with:

FACT TABLES
- gold.fact_sales
- gold.fact_logistics
- gold.fact_order_items

DIMENSION TABLES
- gold.dim_customer
- gold.dim_seller
- gold.dim_product
- gold.dim_date

It includes auditing for row counts, execution time, and error handling.

To execute:
CALL gold.load_gold();
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------*/


CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_ts TIMESTAMP;
    v_end_ts TIMESTAMP;
    v_error_msg TEXT;
    v_error_detail TEXT;
    v_rows_inserted BIGINT;
BEGIN

----------------------------------------------------------------------------------------
-- DROP FACT TABLES FIRST
----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold.fact_order_items;
DROP TABLE IF EXISTS gold.fact_logistics;
DROP TABLE IF EXISTS gold.fact_sales;

----------------------------------------------------------------------------------------
-- DROP DIMENSIONS SECOND
----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold.dim_customer;
DROP TABLE IF EXISTS gold.dim_seller;
DROP TABLE IF EXISTS gold.dim_product;
DROP TABLE IF EXISTS gold.dim_date;
----------------------------------------------------------------------------------------
-- 1. DIM_CUSTOMER
----------------------------------------------------------------------------------------

BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.dim_customer;

    CREATE TABLE gold.dim_customer AS
    SELECT DISTINCT
        c.customer_id,
        c.customer_city,
        c.customer_state,
        c.customer_zip,
        c.customer_city || ', ' || c.customer_state || ', ' || c.customer_zip AS powerbi_cust_map_address
    FROM silver.customers c;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.dim_customer
    ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_customer',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.dim_customer loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_customer',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table dim_customer Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 2. DIM_SELLER
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.dim_seller;

    CREATE TABLE gold.dim_seller AS
    SELECT DISTINCT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_zip
    FROM silver.sellers s;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.dim_seller
    ADD CONSTRAINT pk_dim_seller PRIMARY KEY (seller_id);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_seller',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.dim_seller loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_seller',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table dim_seller Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 3. DIM_PRODUCT
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.dim_product;

    CREATE TABLE gold.dim_product AS
    SELECT DISTINCT
        p.product_id,
        p.product_category,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.volume_cm3
    FROM silver.products p;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.dim_product
    ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_id);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_product',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.dim_product loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_product',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table dim_product Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 4. DIM_DATE
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.dim_date;

    CREATE TABLE gold.dim_date AS
    SELECT DISTINCT
        d::date AS date_key,
        EXTRACT(YEAR FROM d) AS year_num,
        EXTRACT(MONTH FROM d) AS month_num,
        EXTRACT(DAY FROM d) AS day_num,
        TO_CHAR(d, 'Month') AS month_name,
        TO_CHAR(d, 'Day') AS weekday_name,
        EXTRACT(QUARTER FROM d) AS quarter_num
    FROM generate_series(
        (SELECT MIN(purchase_ts)::date FROM silver.orders),
        (SELECT MAX(purchase_ts)::date FROM silver.orders),
        interval '1 day'
    ) d;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.dim_date
    ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_date',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.dim_date loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'dim_date',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table dim_date Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 5. FACT_SALES
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.fact_sales;

    CREATE TABLE gold.fact_sales AS
    WITH aggregated_payments AS (
        SELECT
            order_id,
            SUM(payment_value) AS total_amount_paid,
            MAX(payment_seq) AS total_installment_number
        FROM silver.order_payments
        GROUP BY order_id
    ), aggregated_order_items AS (
		SELECT
			order_id,
			SUM(item_price) as total_item_sales,
			SUM(freight_price) as total_freight,
			SUM(total_item_value) as total_cost
		FROM silver.order_items
		GROUP BY order_id
	)
    SELECT
        o.order_id,
        o.customer_id,
        o.purchase_ts::date AS purchase_date_key,

        aoi.total_item_sales,
        aoi.total_freight,
        aoi.total_cost,

        COALESCE(ap.total_amount_paid, 0) AS total_amount_paid,
        COALESCE(ap.total_installment_number, 0) AS total_installment_number,

        o.order_status

    FROM silver.orders o
    LEFT JOIN aggregated_order_items aoi
        ON aoi.order_id = o.order_id
    LEFT JOIN aggregated_payments ap
        ON o.order_id = ap.order_id;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.fact_sales
    ADD CONSTRAINT fk_fact_sales_customer
    FOREIGN KEY (customer_id)
    REFERENCES gold.dim_customer(customer_id);

    ALTER TABLE gold.fact_sales
    ADD CONSTRAINT fk_fact_sales_date
    FOREIGN KEY (purchase_date_key)
    REFERENCES gold.dim_date(date_key);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_sales',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.fact_sales loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_sales',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table fact_sales Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 6. FACT_LOGISTICS
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.fact_logistics;

    CREATE TABLE gold.fact_logistics AS
    SELECT
        oi.order_id,
        o.customer_id,
        oi.seller_id,
        oi.product_id,

        o.purchase_ts::date AS purchase_date,
        o.delivered_customer_ts::date AS delivered_date,
        o.estimated_ts::date AS estimated_delivery_date,
        o.delivered_carrier_ts::date as delivered_carrier_date,
        o.approval_ts::date as approval_date,

        o.order_status,

        o.seller_to_carrier_lead_time,
        o.lead_time_days,
        o.delay_days,
        o.is_late_int,

        p.volume_cm3,

        oi.item_price,
        oi.freight_price,
        oi.total_item_value AS total_cost,

        ROUND(
            oi.freight_price /
            NULLIF(oi.item_price, 0),
            4
        ) AS freight_ratio

    FROM silver.order_items oi
    LEFT JOIN silver.orders o
        ON oi.order_id = o.order_id
    LEFT JOIN silver.products p
        ON oi.product_id = p.product_id;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.fact_logistics
    ADD CONSTRAINT fk_fact_logistics_customer
    FOREIGN KEY (customer_id)
    REFERENCES gold.dim_customer(customer_id);

    ALTER TABLE gold.fact_logistics
    ADD CONSTRAINT fk_fact_logistics_seller
    FOREIGN KEY (seller_id)
    REFERENCES gold.dim_seller(seller_id);

    ALTER TABLE gold.fact_logistics
    ADD CONSTRAINT fk_fact_logistics_product
    FOREIGN KEY (product_id)
    REFERENCES gold.dim_product(product_id);

    ALTER TABLE gold.fact_logistics
    ADD CONSTRAINT fk_fact_logistics_purchase_date
    FOREIGN KEY (purchase_date)
    REFERENCES gold.dim_date(date_key);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_logistics',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.fact_logistics loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_logistics',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table fact_logistics Load Failed: %', v_error_msg;
END;

----------------------------------------------------------------------------------------
-- 7. FACT_ORDER_ITEMS
----------------------------------------------------------------------------------------
BEGIN

    v_start_ts := clock_timestamp();

    DROP TABLE IF EXISTS gold.fact_order_items;

    CREATE TABLE gold.fact_order_items AS
    SELECT
        oi.order_id,
        oi.item_seq,

        o.customer_id,
        oi.product_id,
        oi.seller_id,

        o.purchase_ts::date AS purchase_date_key,

        oi.item_price,
        oi.freight_price,
        oi.total_item_value,

        o.order_status

    FROM silver.order_items oi
    LEFT JOIN silver.orders o
        ON oi.order_id = o.order_id;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT pk_fact_order_items
    PRIMARY KEY (order_id, item_seq);

    ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_order_items_customer
    FOREIGN KEY (customer_id)
    REFERENCES gold.dim_customer(customer_id);

    ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_order_items_product
    FOREIGN KEY (product_id)
    REFERENCES gold.dim_product(product_id);

    ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_order_items_seller
    FOREIGN KEY (seller_id)
    REFERENCES gold.dim_seller(seller_id);

    ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_order_items_date
    FOREIGN KEY (purchase_date_key)
    REFERENCES gold.dim_date(date_key);

    v_end_ts := clock_timestamp();

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        rows_inserted,
        start_ts,
        end_ts,
        duration_seconds,
        status
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_order_items',
        v_rows_inserted,
        v_start_ts,
        v_end_ts,
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
        'SUCCESS'
    );

    RAISE NOTICE 'Table gold.fact_order_items loaded in % seconds',
        EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_error_msg = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO logging.load_stats (
        process_name,
        table_name,
        start_ts,
        end_ts,
        status,
        executed_by
    )
    VALUES (
        'gold.load_gold_star_schema',
        'fact_order_items',
        v_start_ts,
        clock_timestamp(),
        'FAILED: ' || v_error_msg,
        CURRENT_USER
    );

    RAISE WARNING 'Gold Table fact_order_items Load Failed: %', v_error_msg;
END;

COMMIT;

END;
$$;

/*
## Resulting Star Schema

### Dimension Tables

* gold.dim_customer
* gold.dim_seller
* gold.dim_product
* gold.dim_date

### Fact Tables

* gold.fact_sales
* gold.fact_logistics
* gold.fact_order_items

## Notes

* Grain of sales and logistic fact tables is at the order-item level.
* Grain of order_items fact table is per order per item level.
*/
