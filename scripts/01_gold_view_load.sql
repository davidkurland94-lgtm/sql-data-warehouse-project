/*--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
This script creates the Stored Procedure to load the GOLD layer of the Olist Dataset.
It includes auditing for row counts, execution time, and error handling for each view.

To execute: CALL gold.load_gold_views();
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------*/

create or replace procedure gold.load_gold_views() language plpgsql as $$
declare 
	v_start_ts TIMESTAMP;
    v_end_ts TIMESTAMP;
	v_error_msg TEXT;
    v_error_detail TEXT;
begin
----------------------------------------------------------------------------------------
    -- 1. PAYMENT INSTALLMENT AGGREGATION VIEW
----------------------------------------------------------------------------------------

--This view provides a count of orders along with their value catagorized by payment installment numbers.
	BEGIN
	
		v_start_ts := clock_timestamp();

		DROP MATERIALIZED VIEW IF EXISTS gold.mv_payment_installment_agg;
		
		CREATE MATERIALIZED VIEW gold.mv_payment_installment_agg as
		SELECT 
			installments,
		    order_count,
		    avg_order_value,
		    total_order_value,
		    round(order_count::numeric / sum(order_count) OVER () * 100::numeric, 2) AS pct_of_total_volume
		FROM ( 
				SELECT 
					op.installments,
			        count(DISTINCT o.order_id) AS order_count,
			        sum(op.payment_value::numeric) AS total_order_value,
			        round(avg(op.payment_value::numeric), 2) AS avg_order_value
			    FROM silver.order_payments op
			    JOIN silver.orders o ON o.order_id = op.order_id
			    GROUP BY op.installments) t
		ORDER BY (round(order_count::numeric / sum(order_count) OVER () * 100::numeric, 2)) DESC;
	
		v_end_ts := clock_timestamp();
		
		-- Log successful creation (Note: rows_inserted is NULL or 0 for views)
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
	            'gold.load_gold_views',
	            'mv_payment_installment_agg',
	            0, 
	            v_start_ts,
	            v_end_ts,
	            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
	            'SUCCESS'
	        );
	
	        RAISE NOTICE 'View gold.v_payment_installment_agg refreshed in % seconds', 
	            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));
	
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
	        
	        INSERT INTO logging.load_stats (
	            process_name, 
	            table_name, 
	            start_ts, 
	            end_ts, 
	            status, 
	            executed_by
	        )
	        VALUES (
	            'gold.load_gold_views', 
	            'mv_payment_installment_agg', 
	            v_start_ts, 
	            clock_timestamp(), 
	            'FAILED: ' || v_error_msg, 
	            CURRENT_USER
	        );
	        
	        RAISE WARNING 'Gold View mv_payment_installment_agg Creation Failed: %', v_error_msg;
    END;

----------------------------------------------------------------------------------------
    -- 2. LOGISTICS VIEW
----------------------------------------------------------------------------------------
	BEGIN
	
		v_start_ts := clock_timestamp();
		
		DROP MATERIALIZED VIEW IF EXISTS gold.mv_logistics_kpi;
		
	CREATE MATERIALIZED VIEW gold.mv_logistics_kpi AS
	--using a CTE to aggregate order_items data so that the final view shows one record per order
			with ORDER_ITEMS_AGG AS (
				SELECT
					ORDER_ID,
					sum(p.volume_cm3) as volume_cm3,
					sum(oi.item_price) as total_item_price,
	   				sum(oi.freight_price) as total_freight,
					SUM(TOTAL_ITEM_VALUE) AS TOTAL_cost,
					MAX(ITEM_SEQ) AS TOTAL_ITEMS,
					STRING_AGG(DISTINCT P.PRODUCT_CATEGORY, ', ') AS PRODUCT_CATEGORY
				FROM
					SILVER.ORDER_ITEMS OI
				LEFT JOIN SILVER.PRODUCTS P ON P.PRODUCT_ID = OI.PRODUCT_ID
					GROUP BY
							1
			)
	SELECT 
	    distinct o.order_id,
	    o.customer_id,
		o.order_status,
		c.customer_city,
	    c.customer_state,
		c.customer_zip,
		c.customer_city || ', ' || c.customer_state || ', ' || c.customer_zip as powerbi_cust_map_address,

		-- Timestamp Facts
	    o.purchase_ts as purchase_date,
		o.delivered_carrier_ts as seller_to_carrier_date,
		o.approval_ts as purchase_approval_date,
		o.estimated_ts as estimated_delivery_date,
		o.delivered_customer_ts as delivered_date,
	    -- Performance Metrics
		EXTRACT(DAY FROM(o.delivered_carrier_ts - o.purchase_ts)) as purchase_to_carrier_days,
		o.seller_to_carrier_lead_time,
	    o.lead_time_days,
	    o.delay_days,
		o.is_late_int,
	    -- Item Metrics
	    oi_agg.volume_cm3,
	    -- Freight Efficiency
	    oi_agg.total_item_price,
	    oi_agg.total_freight,
		oi_agg.TOTAL_cost,
	    ROUND((oi_agg.total_freight) / NULLIF((oi_agg.total_item_price), 0), 4) AS freight_ratio
	FROM silver.orders o
	LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
	LEFT JOIN ORDER_ITEMS_AGG oi_agg ON o.order_id = oi_agg.order_id;
	
	v_end_ts := clock_timestamp();
	
	-- Log successful creation (Note: rows_inserted is NULL or 0 for views)
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
            'gold.load_gold_views',
            'mv_logistics_kpi',
            0, 
            v_start_ts,
            v_end_ts,
            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
            'SUCCESS'
        );

        RAISE NOTICE 'View gold.mv_logistics_kpi refreshed in % seconds', 
            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (
            process_name, 
            table_name, 
            start_ts, 
            end_ts, 
            status, 
            executed_by
        )
        VALUES (
            'gold.load_gold_views', 
            'mv_logistics_kpi', 
            v_start_ts, 
            clock_timestamp(), 
            'FAILED: ' || v_error_msg, 
            CURRENT_USER
        );
        
        RAISE WARNING 'Gold View mv_logistics_kpi Creation Failed: %', v_error_msg;
    END;


----------------------------------------------------------------------------------------
    -- 3. SALES VIEW
----------------------------------------------------------------------------------------

		BEGIN
	
		v_start_ts := clock_timestamp();
			
			DROP MATERIALIZED VIEW IF EXISTS gold.mv_sales_kpi;
			
			CREATE MATERIALIZED VIEW gold.mv_sales_kpi AS	
				WITH AGGREGATED_PAYMENTS AS (
						SELECT
							ORDER_ID,
							SUM(PAYMENT_VALUE) AS TOTAL_AMOUNT_PAID,
							MAX(PAYMENT_SEQ) AS TOTAL_INSTALLMENT_NUMBER
						FROM
							SILVER.ORDER_PAYMENTS
						GROUP BY
							ORDER_ID
					),
					ORDER_ITEMS_AGG AS (
						SELECT
							ORDER_ID,
							SUM(TOTAL_ITEM_VALUE) AS TOTAL_ITEM_VALUE,
							MAX(ITEM_SEQ) AS TOTAL_ITEMS,
							STRING_AGG(DISTINCT P.PRODUCT_CATEGORY, ', ') AS PRODUCT_CATEGORY
						FROM
							SILVER.ORDER_ITEMS OI1
							LEFT JOIN SILVER.PRODUCTS P ON P.PRODUCT_ID = OI1.PRODUCT_ID
						GROUP BY
							ORDER_ID
					)
				SELECT
					O.ORDER_ID,
					O.CUSTOMER_ID,
					COALESCE(OI.PRODUCT_CATEGORY, 'N/A') AS PRODUCT_CATEGORY,
					C.CUSTOMER_CITY,
					C.CUSTOMER_STATE,
					O.PURCHASE_TS,
					COALESCE(OI.TOTAL_ITEMS, 0) AS TOTAL_ITEMS,
					COALESCE(OI.TOTAL_ITEM_VALUE, 0) AS TOTAL_ITEM_VALUE,
					COALESCE(AP.TOTAL_AMOUNT_PAID, 0) AS TOTAL_AMOUNT_PAID
				FROM
					SILVER.ORDERS O
					LEFT JOIN ORDER_ITEMS_AGG OI ON O.ORDER_ID = OI.ORDER_ID
					LEFT JOIN AGGREGATED_PAYMENTS AP ON O.ORDER_ID = AP.ORDER_ID
					LEFT JOIN SILVER.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID
				ORDER BY
					6 DESC NULLS FIRST;

	v_end_ts := clock_timestamp();
	
	-- Log successful creation (Note: rows_inserted is NULL or 0 for views)
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
            'gold.load_gold_views',
            'mv_sales_kpi',
            0, 
            v_start_ts,
            v_end_ts,
            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts)),
            'SUCCESS'
        );

        RAISE NOTICE 'View gold.mv_sales_kpi refreshed in % seconds', 
            EXTRACT(EPOCH FROM (v_end_ts - v_start_ts));

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
        
        INSERT INTO logging.load_stats (
            process_name, 
            table_name, 
            start_ts, 
            end_ts, 
            status, 
            executed_by
        )
        VALUES (
            'gold.load_gold_views', 
            'mv_sales_kpi', 
            v_start_ts, 
            clock_timestamp(), 
            'FAILED: ' || v_error_msg, 
            CURRENT_USER
        );
        
        RAISE WARNING 'Gold View mv_sales_kpi Creation Failed: %', v_error_msg;
    END;

COMMIT;

END;
$$


