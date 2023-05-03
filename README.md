# Ikmtest

Run the following script in MSSQL Management Server

Script 1
CREATE TABLE customers (
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  phone VARCHAR(20) NULL
);

CREATE TABLE orders (
  id INT NOT NULL PRIMARY KEY,
  customer_id INT NOT NULL,
  total_price DECIMAL(10, 2) NOT NULL,
  created_at DATETIME NOT NULL,
  CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE products (
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(10, 2) NOT NULL
);

Script 2
INSERT INTO customers (id, name, email, phone)
VALUES (1, 'John Doe', 'john.doe@example.com', NULL),
       (2, 'Jane Smith', 'jane.smith@example.com', '555-1234'),
       (3, 'Bob Johnson', 'bob.johnson@example.com', '555-5678');

-- Sample data for the orders table in SQL Server
INSERT INTO orders (id, customer_id, total_price, created_at)
VALUES (1, 1, 100.00, '2022-01-01 10:00:00'),
       (2, 1, 50.00, '2022-01-02 11:00:00'),
       (3, 2, 75.00, '2022-01-03 12:00:00'),
       (4, 3, 200.00, '2022-01-04 13:00:00');

-- Sample data for the products table in SQL Server
INSERT INTO products (id, name, price)
VALUES (1, 'Product A', 10.00),
       (2, 'Product B', 20.00),
       (3, 'Product C', 30.00);
       
Run the following script in PGAdmin

CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  phone VARCHAR(20) DEFAULT 'N/A'
);

CREATE TABLE orders (
  id INTEGER NOT NULL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  total_price NUMERIC(10, 2) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE products (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  price NUMERIC(10, 2) NOT NULL
);


#Clone the Repository 

Run The following command in cmd to install dependencies

pip install -r requirements.txt



