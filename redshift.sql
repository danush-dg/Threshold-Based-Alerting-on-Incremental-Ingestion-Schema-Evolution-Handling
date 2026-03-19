-- 1. Source Table
CREATE TABLE IF NOT EXISTS source_sales (
    id INT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at TIMESTAMP
);

-- 2. Staging Table
CREATE TABLE IF NOT EXISTS stg_sales (
    id INT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at TIMESTAMP
);

-- 3. Operational Table
CREATE TABLE IF NOT EXISTS op_sales (
    id INT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at TIMESTAMP
);

-- 4. Control Table
CREATE TABLE IF NOT EXISTS control_table (
    id INT IDENTITY(1,1),
    source VARCHAR(50),
    source_table VARCHAR(100),
    staging_table VARCHAR(100),
    operational_table VARCHAR(100),
    ingestion_type VARCHAR(50),
    last_loaded TIMESTAMP,
    primary_key_col VARCHAR(50)
);

-- 5. Logging Table
CREATE TABLE IF NOT EXISTS logging_table (
    id INT IDENTITY(1,1),
    alert VARCHAR(5),
    last_loaded TIMESTAMP,
    stg_tablename VARCHAR(100),
    row_loaded INT,
    status VARCHAR(20),
    schema_change VARCHAR(5)
);
--6 Monitorin TABLE
CREATE TABLE IF NOT EXISTS monitoring_table (
    id INT IDENTITY(1,1),
    current_rows INT,
    avg_rows FLOAT,
    deviation_percent FLOAT,
    created_at TIMESTAMP DEFAULT GETDATE()
);

INSERT INTO control_table 
(source, source_table, staging_table, operational_table, ingestion_type, last_loaded, primary_key_col)
VALUES 
('redshift', 'source_sales', 'stg_sales', 'op_sales', 'incremental', GETDATE() - INTERVAL '2 days', 'id');

INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(1, 'Laptop', 40000.00, GETDATE()),
(2, 'Keyboard', 900.00, GETDATE()),
(3, 'Mouse', 800.00, GETDATE()),
(4, 'Monitor', 15000.00, GETDATE());
INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(5, 'Tablet', 12000.00, GETDATE()),
(6, 'Speaker', 2500.00, GETDATE()),
(7, 'Router', 3500.00, GETDATE()),
(8, 'USB Cable', 300.00, GETDATE());
INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(9, 'SSD', 5000.00, GETDATE()),
(10, 'Projector', 30000.50, GETDATE()),
(11, 'TV', 60000.00, GETDATE()),
(12, 'AC', 50000.00, GETDATE()),
(13, 'Headphones', 2000.00, GETDATE()),
(14, 'Camera', 45000.00, GETDATE()),
(15, 'Printer', 12000.00, GETDATE());

ALTER TABLE source_sales ADD COLUMN category VARCHAR(50);
INSERT INTO source_sales (id, name, amount, updated_at,category) VALUES
(2, 'AC', 50000.00, GETDATE(),'Electronics');


ALTER TABLE source_sales DROP COLUMN category;

ALTER TABLE stg_sales DROP COLUMN category;
ALTER TABLE op_sales DROP COLUMN category;


SELECT * FROM source_sales;
SELECT * FROM stg_sales;
SELECT * FROM op_sales ;
SELECT * FROM logging_table;
SELECT * FROM control_table;
