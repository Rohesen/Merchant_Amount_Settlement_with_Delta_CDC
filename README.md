# Merchant_Amount_Settlement_with_Delta_CDC

This project simulates **real-time UPI transaction processing** using **Databricks, Delta Lake, and Spark Structured Streaming** with **Change Data Capture (CDC)** enabled.
It processes merchant payment transactions, aggregates them in real-time, and maintains up-to-date settlement data.

---

## 🚀 Tech Stack

* **Databricks**
* **Apache Spark (Structured Streaming)**
* **Delta Lake (with Change Data Feed)**
* **PySpark**
* **SQL Data Warehouse (Serverless Compute)**

---

## 📂 Project Overview

The project consists of **two main notebooks**:

### **1. `upi_merchant_pay_trx_mock_data`**

* **Creates the raw Delta table** with CDC enabled.
* Generates **mock UPI transactions** in multiple batches to simulate:

  * New transactions
  * Updates (e.g., status changes from `initiated` → `completed` / `failed`)
  * Refund scenarios
* Uses **MERGE** to upsert data into the Delta table to enable CDC tracking.

#### Raw Table Schema:

| Column                  | Type      | Description                                                         |
| ----------------------- | --------- | ------------------------------------------------------------------- |
| `transaction_id`        | STRING    | Unique transaction identifier                                       |
| `upi_id`                | STRING    | Payer's UPI ID                                                      |
| `merchant_id`           | STRING    | Merchant identifier                                                 |
| `transaction_amount`    | DOUBLE    | Transaction amount                                                  |
| `transaction_timestamp` | TIMESTAMP | Time of transaction                                                 |
| `transaction_status`    | STRING    | Transaction status (`initiated`, `completed`, `failed`, `refunded`) |

#### Example CDC Events:

* **Insert:** New payment initiated
* **Update:** Status change from `initiated` to `completed`/`failed`
* **Refund:** Mark transaction as `refunded`

---

### **2. `cdc_merchant_aggregation`**

* Reads **CDC feed** from the raw UPI transactions table.
* **Filters** only relevant change types:

  * `insert`
  * `update_postimage`
* **Aggregates** data by `merchant_id`:

  * `total_sales` (sum of completed transactions)
  * `total_refunds` (negative sum of refunded amounts)
  * `net_sales` (total\_sales + total\_refunds)
* **Upserts aggregated results** into a **target Delta table** using `MERGE`.

#### Aggregated Table Schema:

| Column          | Type   | Description                     |
| --------------- | ------ | ------------------------------- |
| `merchant_id`   | STRING | Merchant identifier             |
| `total_sales`   | DOUBLE | Total successful payment amount |
| `total_refunds` | DOUBLE | Total refund amount (negative)  |
| `net_sales`     | DOUBLE | Net settlement amount           |

---

## 🔄 Processing Flow

1. **Raw Delta Table Creation**

   * CDC feed enabled for change tracking.

2. **Mock Data Generation**

   * Batch inserts/updates simulate real-time transaction events.

3. **CDC Feed Consumption**

   * Spark Structured Streaming reads changes in micro-batches.

4. **Aggregation Logic**

   * Group by merchant.
   * Calculate sales, refunds, and net settlement.

5. **Upsert to Target Table**

   * Merge aggregated results, keeping data fresh.

6. **Query via SQL Warehouse**

   * Real-time settlement reports for merchants.

---

## 📜 Example SQL Query for Settlement Report

```sql
SELECT *
FROM incremental_load.default.aggregated_upi_transactions
ORDER BY net_sales DESC;
```

---

## 📊 Example CDC Event Table (Console Output)

| transaction\_id | merchant\_id | amount | status    | \_change\_type    |
| --------------- | ------------ | ------ | --------- | ----------------- |
| T001            | M001         | 500.0  | completed | update\_postimage |
| T002            | M002         | 1000.0 | failed    | update\_postimage |
| T004            | M004         | 2000.0 | initiated | insert            |

---

## 🏗 How to Run

1. **Clone repository** and open notebooks in **Databricks**.
2. Run `upi_merchant_pay_trx_mock_data` notebook to:

   * Create raw table
   * Simulate transactions with multiple batches
3. Run `cdc_merchant_aggregation` notebook to:

   * Stream CDC events
   * Aggregate and upsert merchant settlement data
4. Query aggregated results from **SQL Warehouse**.

---

## 📌 Key Features

* **Delta Lake CDC Feed** for incremental processing
* **Streaming Aggregation** for real-time merchant settlements
* **Upsert with Merge** to maintain latest aggregates
* **Refund Handling** in sales computation
* **Databricks-native Workflow** for industrial-scale CDC

---

## 📜 License

This project is licensed under the MIT License.

---
