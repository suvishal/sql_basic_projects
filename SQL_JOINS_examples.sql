-- Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    contact VARCHAR(100),
    address VARCHAR(255)
);

-- Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2)
);

-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date DATE
);

-- Order_Details table
CREATE TABLE order_details (
    order_detail_id INT PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    price DECIMAL(10, 2)
);

-- Sample Data

-- Inserting sample data into customers table
INSERT INTO customers (customer_id, customer_name, contact, address) VALUES 
(1, 'Alice', 'alice@example.com', '123 Main St'),
(2, 'Bob', 'bob@example.com', '456 Elm St');

-- Inserting sample data into products table
INSERT INTO products (product_id, product_name, category, price) VALUES 
(1, 'Laptop', 'Electronics', 1000.00),
(2, 'Smartphone', 'Electronics', 500.00),
(3, 'Desk Chair', 'Furniture', 150.00);

-- Inserting sample data into orders table
INSERT INTO orders (order_id, customer_id, order_date) VALUES 
(1, 1, '2024-01-01'),
(2, 2, '2024-01-02');

-- Inserting sample data into order_details table
INSERT INTO order_details (order_detail_id, order_id, product_id, quantity, price) VALUES 
(1, 1, 1, 2, 2000.00),
(2, 1, 2, 1, 500.00),
(3, 2, 3, 1, 150.00);


-- Inner Join: Retrieve orders along with customer and product details.

SELECT 
    c.customer_name,
    o.order_id,
    o.order_date,
    p.product_name,
    od.quantity,
    od.price
FROM 
    orders o
JOIN 
    customers c ON o.customer_id = c.customer_id
JOIN 
    order_details od ON o.order_id = od.order_id
JOIN 
    products p ON od.product_id = p.product_id
ORDER BY 
    o.order_date;

-- Left Join: Get all customers and their orders, including those who haven't placed any orders.

SELECT 
    c.customer_name,
    o.order_id,
    o.order_date,
    p.product_name,
    od.quantity,
    od.price
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
LEFT JOIN 
    order_details od ON o.order_id = od.order_id
LEFT JOIN 
    products p ON od.product_id = p.product_id
ORDER BY 
    c.customer_name;

-- Right Join: Get all products and the details of orders associated with them, including products that haven't been ordered.

SELECT 
    p.product_name,
    od.quantity,
    od.price,
    o.order_id,
    o.order_date,
    c.customer_name
FROM 
    products p
RIGHT JOIN 
    order_details od ON p.product_id = od.product_id
RIGHT JOIN 
    orders o ON od.order_id = o.order_id
RIGHT JOIN 
    customers c ON o.customer_id = c.customer_id
ORDER BY 
    p.product_name;

-- Full Outer Join: Retrieve all orders and products, even if they don't match

SELECT 
    c.customer_name,
    o.order_id,
    o.order_date,
    p.product_name,
    od.quantity,
    od.price
FROM 
    orders o
FULL OUTER JOIN 
    customers c ON o.customer_id = c.customer_id
FULL OUTER JOIN 
    order_details od ON o.order_id = od.order_id
FULL OUTER JOIN 
    products p ON od.product_id = p.product_id
ORDER BY 
    c.customer_name, p.product_name;

-- Aggregation Functions
-- Total Sales Per Product

SELECT 
    p.product_name,
    SUM(od.price * od.quantity) AS total_sales
FROM 
    products p
JOIN 
    order_details od ON p.product_id = od.product_id
GROUP BY 
    p.product_name
ORDER BY 
    total_sales DESC;

-- Total Orders Per Customer
SELECT 
    c.customer_name,
    COUNT(o.order_id) AS total_orders
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_name
ORDER BY 
    total_orders DESC;

-- Generating Monthly Sales Reports

CREATE OR REPLACE PROCEDURE generate_monthly_sales_report()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE monthly_sales AS
    SELECT 
        DATE_TRUNC('month', o.order_date) AS month,
        p.product_name,
        SUM(od.price * od.quantity) AS total_sales
    FROM 
        orders o
    JOIN 
        order_details od ON o.order_id = od.order_id
    JOIN 
        products p ON od.product_id = p.product_id
    GROUP BY 
        month, p.product_name
    ORDER BY 
        month, total_sales DESC;
    
    -- Output the report (for illustration; adapt as needed)
    SELECT * FROM monthly_sales;
    
    -- Clean up temporary table
    DROP TABLE monthly_sales;
END;
$$;

CALL generate_monthly_sales_report();

-- Automatically Update Inventory When a New Order is Placed

-- Assuming you have an inventory table
CREATE TABLE inventory (
    product_id INT PRIMARY KEY,
    stock INT
);

-- Sample data for inventory
INSERT INTO inventory (product_id, stock) VALUES 
(1, 50),
(2, 100),
(3, 75);

-- Trigger function to update inventory
CREATE OR REPLACE FUNCTION update_inventory()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE inventory
    SET stock = stock - NEW.quantity
    WHERE product_id = NEW.product_id;
    RETURN NEW;
END;
$$;

-- Create trigger for the order_details table
CREATE TRIGGER after_insert_order_details
AFTER INSERT ON order_details
FOR EACH ROW
EXECUTE FUNCTION update_inventory();

