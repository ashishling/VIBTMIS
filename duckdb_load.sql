-- DuckDB table creation and data loading script
-- Generated for BTC store MIS data

-- Create the table with appropriate data types
CREATE TABLE IF NOT EXISTS mis_long (
    store_name TEXT,
    parameter TEXT,
    cafe_code TEXT,
    region TEXT,
    category TEXT,
    for_ssg TEXT,
    area_store DOUBLE,
    store_start_date DATE,
    vintage TEXT,
    month DATE,
    value DOUBLE
);

-- Load data from CSV
COPY mis_long FROM '/Users/ashishlingamneni/Cursor Projects/BTCDuckDB/clean_mis_long.csv' (HEADER, AUTO_DETECT TRUE);

-- Verify data loaded correctly
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT store_name) as unique_stores,
    COUNT(DISTINCT parameter) as unique_parameters,
    MIN(month) as earliest_month,
    MAX(month) as latest_month
FROM mis_long;

-- Example queries:

-- 1. Revenue by region for 2024
-- SELECT 
--     region,
--     SUM(value) as total_revenue
-- FROM mis_long 
-- WHERE parameter = 'Revenue' 
--   AND month BETWEEN '2024-01-01' AND '2024-12-31'
-- GROUP BY region 
-- ORDER BY total_revenue DESC;

-- 2. Average EBITDA margin by store
-- SELECT 
--     store_name,
--     AVG(value) as avg_ebitda_margin
-- FROM mis_long 
-- WHERE parameter = '%'
-- GROUP BY store_name 
-- ORDER BY avg_ebitda_margin DESC;

-- 3. Monthly transaction trends
-- SELECT 
--     month,
--     SUM(value) as total_transactions
-- FROM mis_long 
-- WHERE parameter = 'Transactions'
-- GROUP BY month 
-- ORDER BY month;

-- 4. Store performance comparison
-- SELECT 
--     store_name,
--     region,
--     category,
--     SUM(CASE WHEN parameter = 'Revenue' THEN value ELSE 0 END) as revenue,
--     SUM(CASE WHEN parameter = 'EBITDA' THEN value ELSE 0 END) as ebitda,
--     AVG(CASE WHEN parameter = '%' THEN value ELSE NULL END) as margin
-- FROM mis_long 
-- WHERE month >= '2024-01-01'
-- GROUP BY store_name, region, category
-- ORDER BY revenue DESC;
