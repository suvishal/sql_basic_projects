-- Simulate Incoming Data Stream
-- This table acts as the source for incoming data simulating a real-time data stream
-- It contains columns like stock_id, price, and timestamp to store updates

CREATE TABLE stock_updates (
    stock_id INT,
    price DECIMAL(10, 2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inserting Sample Data
-- This simulates incoming data points

INSERT INTO stock_updates (stock_id, price) VALUES 
(1, 100.50),
(2, 150.75),
(1, 101.00),
(3, 200.30);

-- Staging Table for Data Transformation
-- A temporary table to hold a subset of recent data for processing

CREATE TEMPORARY TABLE stock_staging (
    stock_id INT,
    price DECIMAL(10, 2),
    timestamp TIMESTAMP
);

-- Loading Data
-- The INSERT query filters data received in the last 5 minutes
-- Adjust the interval as needed

INSERT INTO stock_staging (stock_id, price, timestamp)
SELECT stock_id, price, timestamp
FROM stock_updates
WHERE timestamp >= NOW() - INTERVAL '5 MINUTES';

-- Transformation Using Window Functions
-- Create a table with a transformation logic that identifies the most recent record for each stock_id

CREATE TEMPORARY TABLE stock_transformed AS
SELECT 
    stock_id,
    price,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY timestamp DESC) AS row_num
FROM stock_staging;

-- The ROW_NUMBER() function helps pick the latest record by ordering by timestamp within each stock_id

-- Loading Data into the Final Table
-- This table holds up-to-date stock data with unique stock_id

CREATE TABLE IF NOT EXISTS final_stock_data (
    stock_id INT PRIMARY KEY,
    latest_price DECIMAL(10, 2),
    last_updated TIMESTAMP
);

-- Upsert Logic
-- Inserts new records and updates existing ones using ON CONFLICT

INSERT INTO final_stock_data (stock_id, latest_price, last_updated)
SELECT stock_id, price, timestamp
FROM stock_transformed
WHERE row_num = 1
ON CONFLICT (stock_id) DO UPDATE SET 
    latest_price = EXCLUDED.latest_price,
    last_updated = EXCLUDED.last_updated;

-- Cleanup
-- Drop temporary tables after loading is complete to free up resources

DROP TABLE IF EXISTS stock_staging, stock_transformed;

-- * * * * * psql -U username -d database -f /path/to/etl_pipeline_for_realtime_data_intake.sql for job scheduler like cron (Linux) or SQL Agent (SQL Server) to automate the execution at regular intervals;
