-- Create products table
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    price NUMERIC
);

-- Create product_sales table
CREATE TABLE product_sales (
    id INTEGER PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    sale_date DATE
);

-- Create product_sales_summary table
CREATE TABLE product_sales_summary (
    product_id INTEGER PRIMARY KEY REFERENCES products(id),
    total_quantity INTEGER
);

-- Insert sample data into products table
INSERT INTO products (id, name, price)
VALUES (1, 'Product A', 10.0), (2, 'Product B', 20.0), (3, 'Product C', 30.0);

-- Insert sample data into product_sales table
INSERT INTO product_sales (id, product_id, quantity, sale_date)
VALUES (1, 1, 5, '2023-04-25'), (2, 2, 3, '2023-04-25'), (3, 3, 10, '2023-04-26'), (4, 1, 7, '2023-04-27');