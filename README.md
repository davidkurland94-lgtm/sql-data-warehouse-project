# SQL Data Warehouse Project

Transform raw Brazilian e-commerce data into a modern data warehouse using PostgreSQL. This project consolidates messy CSV logs into a **Star Schema**—a clean, organized database structure that makes analysis fast and intuitive.

## What is a Star Schema?

Think of it like organizing a messy filing cabinet:
- **Before:** You have 7 different CSV files scattered around, each with overlapping info. Finding "How much did customer X spend?" means checking multiple files.
- **After:** You have one central "Fact" table (like an index card for every order), connected to organized lookup tables (Customers, Products, Dates). Questions are answered instantly.

This project automates that entire transformation using **three layers** of PostgreSQL scripts, each making the data cleaner and more useful.

---

## The Three-Layer Pipeline

Your data flows through three layers, each with a specific job:

### Layer 1: **Bronze** (Raw Data)
**What it does:** Takes the messy CSV files and loads them as-is into PostgreSQL.

**Script:** `load_bronze.sql`

Think of this like dumping all your receipts into a box—nothing is organized yet, but it's all there. The bronze layer:
- Preserves the original data exactly (no cleaning)
- Adds a `loaded_at` timestamp to track when data arrived
- Uses simple table names like `bronze_orders`, `bronze_customers`

---

### Layer 2: **Silver** (Cleaned & Standardized)
**What it does:** Fixes data quality issues and creates a standard format.

**Scripts:** `load_silver.sql` (main transformation)

This is where the real work happens. Silver layer:
- **Removes duplicates** — Multiple orders with the same ID? Only keep one.
- **Fixes data types** — Turns text that looks like dates into actual dates (PostgreSQL understands `2018-01-01` better than `"01 jan 2018"`)
- **Handles missing values** — Decides whether to fill gaps or exclude incomplete records
- **Creates keys** — Adds `customer_id`, `product_id` so tables can connect later

**Key insight about customers:** The silver layer uses `customer_unique_id` to identify repeat customers (since the same person can have different `customer_id` values across orders).

---

### Layer 3: **Gold** (Analysis-Ready)
**What it does:** Creates the final Star Schema and summary tables ready for dashboards.

**Scripts:** 
- `load_gold.sql` — Builds the core fact and dimension tables
- `01_gold_view_load.sql` — Creates easy-to-query views on top

The gold layer creates a **Star Schema** that looks like this:

```
                 ┌──────────────┐
                 │ dim_customers│
                 │ ├ customer_id│
                 │ ├ name       │
                 │ └ city       │
                 └──────┬───────┘
                        │
    ┌─────────────┐     │
    │ dim_dates   │     │
    │ ├ date_id   │     │     ┌─────────────────┐
    │ └ month     │─────┼─────│ fact_orders     │─────┐
    └─────────────┘     │     │ ├ order_id      │     │
                        │     │ ├ customer_id   │     │
    ┌──────────────┐     │     │ ├ product_id    │     │
    │ dim_products │     │     │ ├ date_id       │     │
    │ ├ product_id │─────┤     │ ├ quantity      │     │
    │ ├ name       │     │     │ └ revenue       │     │
    │ └ category   │     │     └─────────────────┘     │
    └──────────────┘     │                             │
                         │                             │
                    ┌────┴──────────────────────────┐  │
                    │ agg_orders_by_month_category │  │
                    │ (pre-calculated summaries)   │◄─┘
                    └─────────────────────────────┘
```

**Why this design?**
- The central `fact_orders` table has one row per order — lightweight and fast
- Each dimension table (customers, products, dates) stores descriptive info once — no repetition
- Pre-aggregated tables (`agg_orders_by_month_category`) make dashboards instant

---

## Database Setup Scripts

These scripts initialize the warehouse structure before any data loads:

- **`init_database.sql`** — Creates the PostgreSQL database
- **`init_schemas.sql`** — Creates three separate schema folders (`bronze_schema`, `silver_schema`, `gold_schema`) to keep layers organized

---

## Monitoring & Logging

**`logging_table_load_stats.sql`** — Creates a audit trail tracking:
- How many rows each script loaded
- When each load ran
- How long it took

This is useful if something breaks—you can see exactly where.

---

## The Dataset: Brazilian E-Commerce (Olist)

**Source:** [Kaggle Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**What's in it:**
- **Orders:** 99,000+ orders from a Brazilian marketplace (Jan 2017 - Aug 2018)
- **Customers:** 96,000+ unique customers
- **Products:** 3,100+ products across 73 categories
- **Sellers:** 3,100+ sellers
- **Payments & Shipping:** Details on how customers paid and delivery status

**Original structure:** 8 separate CSV files (orders.csv, customers.csv, etc.)

**Your project's job:** Merge all 8 into one coherent warehouse.

---

## Dashboard

### Overview
![Olist Dashboard Cover](/docs/dashboard-cover.png)

**Key Metrics (as of Aug 2018):**
- **R$15.8M** Gross Revenue
- **99K** Total Orders
- **12 days** Average Lead Time
- **6.5%** Late Delivery Rate
- **3.1K** Number of Sellers

**Visualizations include:**
- Sales trends by month (comparing 2017 vs 2018)
- Orders by location (geographic heatmap across Brazil)
- Category performance (which product categories drive revenue)
- Delivery performance metrics

For the full interactive report, download [Dashboard Report PDF](./docs/Olist_commerce_dashboard.pdf).

---

## Project Structure

```
sql-data-warehouse-project/
├── scripts/
│   ├── init_database.sql              # Create database
│   ├── init_schemas.sql               # Create schema layers
│   ├── load_bronze.sql                # Load raw CSVs
│   ├── load_silver.sql                # Clean & standardize
│   ├── load_gold.sql                  # Build star schema
│   ├── 01_gold_view_load.sql          # Create final views
│   └── logging_table_load_stats.sql   # Track load performance
├── datasets/                          # Raw CSV files (from Kaggle)
├── docs/
│   ├── database_model.png             # ER Diagram (star schema visual)
│   └── dashboard-report.pdf           # PowerBI dashboard export
└── README.md                          # You are here
```

---

## How to Use This Project

### 1. Get the Data
Download the [Kaggle Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and extract the CSV files to `datasets/`.

### 2. Set Up PostgreSQL
```bash
# Run scripts in order
psql -U your_user -f scripts/init_database.sql
psql -U your_user -f scripts/init_schemas.sql
```

### 3. Load & Transform
```bash
# Load each layer in sequence
psql -U your_user -f scripts/load_bronze.sql
psql -U your_user -f scripts/load_silver.sql
psql -U your_user -f scripts/load_gold.sql
psql -U your_user -f scripts/01_gold_view_load.sql
```

### 4. Verify
```bash
# Check what tables were created
psql -U your_user
\dt gold_schema.*    # List all gold schema tables
SELECT * FROM gold_schema.fact_orders LIMIT 5;
```

---

## Key Learnings

### Customer Deduplication
⚠️ **Important:** The raw dataset has a quirk — every order gets a unique `customer_id`, but the same person (customer) might appear multiple times with different IDs. The silver layer uses `customer_unique_id` to correctly identify repeat customers.

### Date Handling
Dates in the raw data come in different formats. The silver layer standardizes everything to PostgreSQL `DATE` type, and the gold layer creates a `dim_dates` dimension table for easy time-based filtering.

### Star Schema Benefits
Once data is in the gold layer:
- **Fast queries** — Joins are simple; the schema is normalized
- **Dashboard-friendly** — Tools like PowerBI connect directly to the gold views
- **Easy to maintain** — Changes to business logic happen in one view, not a hundred dashboards

---

## What's Next?

- [ ] Add incremental loading (only load new orders each day, not re-process everything)
- [ ] Create more pre-aggregated tables for common dashboard queries
- [ ] Add data quality tests (e.g., "revenue should never be negative")
- [ ] Automate script execution with cron jobs or cloud schedulers

---

## Notes

This is a **learning project** showcasing data warehouse fundamentals using PostgreSQL. It demonstrates:
- ✅ ETL pipeline design (Extract, Transform, Load)
- ✅ Star Schema modeling
- ✅ SQL scripting & automation
- ✅ Layers of abstraction (Bronze/Silver/Gold)

**Not production-ready** (no error handling, incremental loading, or data validation), but excellent for understanding how real data warehouses are built.

---

## License

MIT
