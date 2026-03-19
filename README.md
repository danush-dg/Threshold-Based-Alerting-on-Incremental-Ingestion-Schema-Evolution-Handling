# 🚀 AWS Redshift ETL Pipeline with Schema Evolution & Monitoring

## 📌 Overview

This project implements a **production-style ETL pipeline** using:

* **AWS Lambda**
* **Amazon Redshift**
* **AWS Secrets Manager**
* **Amazon CloudWatch**
* **Amazon SNS**

The pipeline supports:

* Incremental data loading
* Schema evolution (add, drop, datatype changes)
* Monitoring & anomaly detection
* Automated alerting

---

## 🏗️ Architecture

```
Source Table (source_sales)
        ↓
Staging Table (stg_sales)
        ↓
Operational Table (op_sales)
        ↓
Logging Table (logging_table)
        ↓
Monitoring Table (monitoring_table)
        ↓
CloudWatch Metrics → SNS Alerts
```

---

## ⚙️ Features

### ✅ 1. Incremental Load

* Loads only new/updated records using `updated_at`
* Controlled via `control_table`

---

### ✅ 2. Schema Evolution Handling

Automatically detects and handles:

#### ➤ New Columns

* Adds to staging and operational tables

#### ➤ Dropped Columns

* Removes from staging and operational tables

#### ➤ Datatype Changes

* Uses safe migration approach:

  * Add new column
  * Copy data
  * Drop old column
  * Rename

---

### ✅ 3. Data Pipeline Flow

1. Extract data from source
2. Load into staging (truncate + insert)
3. Upsert into operational table
4. Update control table
5. Log execution

---

### ✅ 4. Monitoring System

#### 📊 Metrics Calculated:

* Current rows processed
* 5-day rolling average
* Deviation percentage

#### 🧠 Formula:

```
Deviation % = ((current - avg) / avg) * 100
```

---

### ✅ 5. Alert Mechanism

Alerts are triggered when:

```
current_rows > avg_last_5_days * 1.10
OR
schema_change == 'Yes'
```

---

### 🔔 Alert Channels:

* Amazon SNS (Email / Notification)

---

### 📈 CloudWatch Metrics:

* `RowsProcessed`

---

## 🗄️ Required Tables

### 1. Control Table

Tracks pipeline metadata

### 2. Logging Table

Stores execution logs

### 3. Monitoring Table

```sql
CREATE TABLE monitoring_table (
    id INT IDENTITY(1,1),
    current_rows INT,
    avg_rows FLOAT,
    deviation_percent FLOAT,
    created_at TIMESTAMP DEFAULT GETDATE()
);
```

---

## 🔐 Secrets Manager

Stores Redshift credentials:

```json
{
  "host": "...",
  "username": "...",
  "password": "...",
  "database": "..."
}
```

---

## ☁️ AWS Permissions Required

IAM Role must include:

* `secretsmanager:GetSecretValue`
* `cloudwatch:PutMetricData`
* `sns:Publish`

---

## 🚀 Lambda Execution Flow

1. Fetch credentials from Secrets Manager
2. Run ETL pipeline
3. Compute rolling average & deviation
4. Store metrics in monitoring table
5. Push CloudWatch metric
6. Trigger SNS alert if condition met
7. Print final summary

---

## 📊 Sample Output

```
========== ETL SUMMARY ==========
Total Rows Processed : 15
Current Rows         : 15
5-Day Avg            : 10
Deviation (%)        : 50.00
Schema Change        : Yes
Alert Triggered      : Yes
Status               : SUCCESS
================================
```

---

## 🧪 Use Cases

* Incremental ETL pipelines
* Schema drift handling
* Data quality monitoring
* Anomaly detection
* Real-time alerting

---

## 🔥 Future Enhancements

* CloudWatch Dashboard visualization
* Multi-level alert severity
* Data quality checks (nulls, duplicates)
* Partition-based loading
* Integration with S3 / Kinesis

---

## 👨‍💻 Author

Danush

---

## 📌 Summary

This project demonstrates a **real-world ETL system** with:

✔ Schema evolution
✔ Incremental ingestion
✔ Monitoring & anomaly detection
✔ Alerting via AWS

A strong foundation for **production-grade data pipelines**.
