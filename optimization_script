-- Schema Creation with Better Organization and Performance Optimizations
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Enable parallel query execution
SET max_parallel_workers_per_gather = 4;

-- Create custom types for better data integrity
CREATE TYPE product_category AS ENUM ('Electronics', 'Furniture');

-- Create tables with appropriate constraints and comments
CREATE TABLE ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category product_category NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_product_name UNIQUE (product_name)
);

-- Create sales table with partitioning
CREATE TABLE ecommerce.sales (
    sale_id SERIAL,
    product_id INT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    total DECIMAL(10, 2) NOT NULL CHECK (total >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, sale_date)
) PARTITION BY RANGE (sale_date);

-- Create partitions for better query performance
CREATE TABLE ecommerce.sales_2024_q1 PARTITION OF ecommerce.sales
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE ecommerce.sales_2024_q2 PARTITION OF ecommerce.sales
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE ecommerce.sales_2024_q3 PARTITION OF ecommerce.sales
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE ecommerce.sales_2024_q4 PARTITION OF ecommerce.sales
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

-- Add foreign key after table creation
ALTER TABLE ecommerce.sales 
    ADD CONSTRAINT fk_sales_product 
    FOREIGN KEY (product_id) 
    REFERENCES ecommerce.products(product_id) 
    ON DELETE RESTRICT;

-- Create optimized indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_product_id 
    ON ecommerce.sales (product_id, sale_date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_date 
    ON ecommerce.sales (sale_date DESC, total);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_category 
    ON ecommerce.products (category, price);

-- Create function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for timestamp management
CREATE TRIGGER update_products_timestamp
    BEFORE UPDATE ON ecommerce.products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- Insert sample data with error handling
DO $$
BEGIN
    INSERT INTO ecommerce.products (product_id, product_name, category, price) 
    VALUES 
        (1, 'Laptop', 'Electronics', 1000.00),
        (2, 'Smartphone', 'Electronics', 500.00),
        (3, 'Desk Chair', 'Furniture', 150.00)
    ON CONFLICT (product_id) DO NOTHING;

    INSERT INTO ecommerce.sales (sale_id, product_id, sale_date, quantity, total) 
    VALUES 
        (1, 1, '2024-01-01', 2, 2000.00),
        (2, 2, '2024-01-02', 3, 1500.00),
        (3, 3, '2024-01-03', 1, 150.00)
    ON CONFLICT (sale_id, sale_date) DO NOTHING;
END $$;

-- Optimized query examples with CTEs and window functions
-- Total sales by product with year-over-year comparison
WITH yearly_sales AS (
    SELECT 
        p.product_name,
        EXTRACT(YEAR FROM s.sale_date) AS sale_year,
        SUM(s.total) AS total_sales,
        COUNT(*) AS number_of_sales
    FROM 
        ecommerce.products p
        JOIN ecommerce.sales s ON p.product_id = s.product_id
    GROUP BY 
        p.product_name, 
        EXTRACT(YEAR FROM s.sale_date)
)
SELECT 
    product_name,
    sale_year,
    total_sales,
    LAG(total_sales) OVER (PARTITION BY product_name ORDER BY sale_year) AS prev_year_sales,
    ROUND(
        ((total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY sale_year)) 
        / LAG(total_sales) OVER (PARTITION BY product_name ORDER BY sale_year) * 100)::numeric, 
        2
    ) AS yoy_growth
FROM 
    yearly_sales
ORDER BY 
    product_name, 
    sale_year;

-- Maintenance commands wrapped in a function
CREATE OR REPLACE FUNCTION ecommerce.maintain_tables()
RETURNS void AS $$
BEGIN
    -- Update statistics
    ANALYZE VERBOSE ecommerce.products;
    ANALYZE VERBOSE ecommerce.sales;
    
    -- Reclaim space and update visibility map
    VACUUM (ANALYZE, VERBOSE) ecommerce.products;
    VACUUM (ANALYZE, VERBOSE) ecommerce.sales;
    
    -- Reindex tables
    REINDEX TABLE CONCURRENTLY ecommerce.products;
    REINDEX TABLE CONCURRENTLY ecommerce.sales;
END;
$$ LANGUAGE plpgsql;

-- Create a more efficient cron job command
-- Replace with actual values:
-- 0 0 * * 0 psql -U username -d database -c "SELECT ecommerce.maintain_tables();"
