CREATE TABLE source_sales (
sale_id INT,
product_name VARCHAR(100),
quantity INT,
price DECIMAL(10,2),
sale_date DATE
);
INSERT INTO source_sales VALUES
(1,'Laptop',2,800.00,'2024-01-01'),
(2,'Mouse',5,20.00,'2024-01-01'),
(3,'Keyboard',3,50.00,'2024-01-02'),
(4,'Monitor',1,300.00,'2024-01-02');

CREATE TABLE control_table (
run_date DATE,
rows_loaded INT,
status VARCHAR(20),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE monitoring_table (
run_date DATE,
current_count INT,
avg_last_5_days FLOAT,
deviation_percent FLOAT
);

CREATE TABLE alerts_table (
alert_id INT IDENTITY(1,1),
run_date DATE,
message VARCHAR(500),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE schema_history (
version_id INT IDENTITY(1,1),
table_name VARCHAR(100),
schema_definition VARCHAR(1000),
change_detected_at TIMESTAMP
);


INSERT INTO control_table VALUES
('2026-03-12', 10, 'SUCCESS', CURRENT_TIMESTAMP),
('2026-03-13', 12, 'SUCCESS', CURRENT_TIMESTAMP),
('2026-03-14', 11, 'SUCCESS', CURRENT_TIMESTAMP),
('2026-03-15', 9, 'SUCCESS', CURRENT_TIMESTAMP),
('2026-03-16', 10, 'SUCCESS', CURRENT_TIMESTAMP);


INSERT INTO source_sales VALUES
(1001,'Laptop',1,900,CURRENT_DATE),
(1002,'Mouse',2,40,CURRENT_DATE),
(1003,'Keyboard',1,80,CURRENT_DATE),
(1004,'Monitor',1,300,CURRENT_DATE),
(1005,'Tablet',2,400,CURRENT_DATE);

INSERT INTO source_sales VALUES
(1006,'Phone',3,700,CURRENT_DATE),
(1007,'Speaker',1,120,CURRENT_DATE),
(1008,'Camera',1,500,CURRENT_DATE),
(1009,'Printer',1,200,CURRENT_DATE),
(1010,'Router',1,150,CURRENT_DATE);

INSERT INTO source_sales VALUES
(1011,'SSD',1,180,CURRENT_DATE),
(1012,'RAM',2,220,CURRENT_DATE);

SELECT *
FROM schema_history
ORDER BY version_id;

SELECT column_name
FROM information_schema.columns
WHERE table_name='source_sales'
ORDER BY ordinal_position;

ALTER TABLE source_sales
ADD COLUMN region VARCHAR(50);

SELECT column_name
FROM information_schema.columns
WHERE table_name='source_sales'
ORDER BY ordinal_position;

INSERT INTO source_sales
VALUES (2001,'Laptop',1,900,CURRENT_DATE,'South');


INSERT INTO source_sales VALUES
(5001,'Laptop',1,900,CURRENT_DATE),
(5002,'Mouse',2,40,CURRENT_DATE),
(5003,'Keyboard',1,80,CURRENT_DATE),
(5004,'Monitor',1,300,CURRENT_DATE),
(5005,'Tablet',2,400,CURRENT_DATE),
(5006,'Phone',3,700,CURRENT_DATE),
(5007,'Speaker',1,120,CURRENT_DATE),
(5008,'Camera',1,500,CURRENT_DATE),
(5009,'Printer',1,200,CURRENT_DATE),
(5010,'Router',1,150,CURRENT_DATE),
(5011,'SSD',1,180,CURRENT_DATE),
(5012,'RAM',2,220,CURRENT_DATE);




