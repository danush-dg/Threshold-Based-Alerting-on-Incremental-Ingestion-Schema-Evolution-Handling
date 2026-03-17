# 📊 Threshold-Based Alerting & Schema Evolution Handling Pipeline

## 🚀 Overview
This project implements a resilient batch ingestion monitoring system that detects data spikes and schema changes. It ensures data reliability and self-healing pipelines using AWS services and SQL-based monitoring.

---

## 🎯 Objectives
- Monitor daily ingestion volume  
- Detect anomalies when: current_count > 5_day_avg × 1.10  
- Trigger alerts automatically  
- Detect schema changes (add/remove columns)  
- Maintain schema history  

---

## ⚙️ Tech Stack
AWS Services:
- EventBridge Scheduler  
- AWS Lambda  
- Amazon Redshift Serverless  
- CloudWatch (Metrics & Alarms)  
- SNS (Notifications)  
- Secrets Manager  

Data Layer:
- SQL (Redshift)  
- Information Schema  

---

## 🧩 Architecture Flow
1. EventBridge triggers Lambda daily  
2. Lambda ingests incremental data  
3. Control table updated  
4. Rolling average computed  
5. Threshold evaluated  
6. CloudWatch metric published  
7. Alarm triggers if needed  
8. SNS sends notification  
9. Schema changes detected  

---

## 📂 Database Schema

### Source Table
```sql
CREATE TABLE source_sales (
    sale_id INT,
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2),
    sale_date DATE
);

Control Table
CREATE TABLE control_table (
    run_date DATE,
    rows_loaded INT,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
Monitoring Table
CREATE TABLE monitoring_table (
    run_date DATE,
    current_count INT,
    avg_last_5_days FLOAT,
    deviation_percent FLOAT
);
Alerts Table
CREATE TABLE alerts_table (
    alert_id INT IDENTITY(1,1),
    run_date DATE,
    message VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
Schema History Table
CREATE TABLE schema_history (
    version_id INT IDENTITY(1,1),
    table_name VARCHAR(100),
    schema_definition VARCHAR(1000),
    change_detected_at TIMESTAMP
);
🔄 Pipeline Phases
Phase 1 — Source Setup

Create source table

Insert sample data

Phase 2 — Monitoring Setup

Create control, monitoring, alerts, schema tables

Phase 3 — Secure Access

Store credentials in Secrets Manager

IAM Role: LambdaIngestionRole

Phase 4 — Daily Ingestion
SELECT COUNT(*)
FROM source_sales
WHERE sale_date = CURRENT_DATE;
Phase 5 — Rolling Average
SELECT AVG(rows_loaded)
FROM control_table
WHERE run_date >= CURRENT_DATE - INTERVAL '5 day';
Phase 6 — Threshold Detection

Condition:

current_count > avg_last_5_days * 1.10
Phase 7 — CloudWatch Monitoring

Publish metric: IngestionSpike = 1

Phase 8 — Notification (SNS)

Email/SNS alert triggered

Phase 9 — Schema Evolution
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'source_sales';
Phase 10 — Scheduling

EventBridge schedule: rate(1 day)

🧠 Lambda Logic

Retrieve credentials

Count rows

Compute average

Calculate deviation

Store metrics

Check threshold

Publish metric

Detect schema change

Log schema updates

🧪 Testing
Threshold Spike Test
INSERT INTO source_sales VALUES
(1001,'Laptop',1,900,CURRENT_DATE),
(1002,'Mouse',2,40,CURRENT_DATE),
(1003,'Keyboard',1,80,CURRENT_DATE),
(1004,'Monitor',1,300,CURRENT_DATE),
(1005,'Tablet',2,400,CURRENT_DATE),
(1006,'Phone',3,700,CURRENT_DATE),
(1007,'Speaker',1,120,CURRENT_DATE),
(1008,'Camera',1,500,CURRENT_DATE);

Expected:

Alert triggered

Entry in alerts_table

Schema Evolution Test
ALTER TABLE source_sales
ADD COLUMN region VARCHAR(50);

Expected:

Schema change detected

Entry in schema_history

✅ Outcome

Automated ingestion monitoring

Real-time anomaly detection

Schema evolution handling

Alerting via CloudWatch + SNS

Reliable and scalable pipeline