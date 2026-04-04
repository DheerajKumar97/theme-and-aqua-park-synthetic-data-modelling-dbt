# 🎢 Qiddiya Theme and Aqua Park Synthetic Data Modeling

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7+-orange.svg)](https://www.getdbt.com/)
[![Databricks](https://img.shields.io/badge/Databricks-SQL-red.svg)](https://databricks.com/)

A comprehensive, end-to-end data engineering solution that generates enterprise-grade synthetic data for a hypothetical theme park (Qiddiya Theme and Aqua Park), uploads it directly to a Databricks SQL Warehouse, and models it using a **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) with **dbt (data build tool)**.

---

## 📖 Table of Contents
- [Project Overview](#-project-overview)
- [Key Features](#-key-features)
- [Technology Stack](#-technology-stack)
- [Architecture](#-architecture)
  - [1. Data Generation Pipeline](#1-data-generation-pipeline)
  - [2. Medallion Data Warehouse (dbt)](#2-medallion-data-warehouse-dbt)
- [Database Schema (Gold Layer)](#-database-schema-gold-layer)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage Guide](#-usage-guide)
  - [Step 1: Data Generation & Bronze Upload](#step-1-data-generation--bronze-upload)
  - [Step 2: dbt Execution (Transformations)](#step-2-dbt-execution-transformations)
- [Testing & Data Quality](#-testing--data-quality)

---

## 🌟 Project Overview

This repository is split into two primary workflows:
1. **Synthetic Data ETL Pipeline**: A robust Python script (`qiddiya_load_databricks.py`) utilizing Pandas and Faker to generate hundreds of thousands of realistic records (transactions, ride operations, weather, gate events, etc.). The script strictly enforces referential integrity, securely injects controlled data anomalies for testing, and streams the raw data into a Databricks Bronze layer.
2. **dbt Databricks Modeling**: A full-fledged dbt project (`dheeraj_qiddiya_dbt_databricks`) that cleans, standardizes, and models the raw Bronze data into Silver (Cleansed) and Gold (Analytical / Star Schema) data layers tailored for BI reporting and analytics.

---

## ✨ Key Features

- **Massive Realistic Data Generation**: Simulates realistic theme park footprints including customers, staff, transactions, park gates, feedback, weather sensors, and SCADA ride metrics.
- **100% Referential Integrity Engine**: Custom validation checks pre-upload to ensure no orphaned foreign keys across millions of scattered records.
- **Smart Data Quality Testing**: Injects controlled data anomalies natively into the Bronze layer (missing values, malformed dates, dupe flags) to thoroughly test the dbt Silver layer cleansing pipeline.
- **Chunked SQL Uploads**: Directly integrates with the `databricks-sql-connector` uploading thousands of rows sequentially for immediate ingestion capabilities.
- **Medallion Architecture in dbt**:
  - **Bronze**: Raw landing tables sourced from Python ingest.
  - **Silver**: Deduped, cleansed, strongly-typed tables.
  - **Gold**: Kimball Methodology Star Schema comprising strictly validated 8 Dimension tables and 5 Fact tables.

---

## 🛠️ Technology Stack

- **Language**: Python 3.11+
- **Data Generator**: `pandas`, `numpy`, `Faker`, `uuid`
- **Data Warehouse**: Databricks (Databricks SQL Warehouse)
- **Database Driver**: `databricks-sql-connector`
- **Transformation Engine**: `dbt-core`, `dbt-databricks`
- **Package Management**: `pyproject.toml`, `uv` / `pip`

---

## 🏗️ Architecture

### 1. Data Generation Pipeline (`qiddiya_load_databricks.py`)
Generates structured datasets corresponding to business events:
- **Phase A (Master Data Pools)**: `Customers`, `Staff`, `Products`.
- **Phase B (Transactional Entities)**: `Transactions`, `Gate Events`, `Ride Operations`, `Feedback`, `Weather`, `Master Data`. 
*(Produces 9 `SF_` (Six Flags) datasets and mutates identically shaped 9 `AQ_` (Aqua Park) datasets)*

### 2. Medallion Data Warehouse (dbt)
Managed entirely inside `dheeraj_qiddiya_dbt_databricks/`.
- **`models/bronze/`**: 1:1 view on top of the ingested RAW ingest data.
- **`models/silver/`**: Handles CAST transformations, boolean normalizations, coalescing missing fields, schema evolution filters, and duplicate record deduplication.
- **`models/gold/`**: Generates complex analytical views structured perfectly for immediate downstream usage.

---

## 🗄️ Database Schema (Gold Layer)

The finalized Gold layer features a highly optimized Star Schema layout:

### Dimension Tables (8)
1. `dim_customer` - Granular customer details, loyalty tiers, PII handling.
2. `dim_date` - Dedicated temporal table for grouping logic.
3. `dim_gate` - Park entry topologies and access points.
4. `dim_park` - Site codes ("SF" vs "AQ").
5. `dim_product` - Hierarchical categorization of POS purchasable metrics.
6. `dim_ride` - Physical roller-coaster / attraction specs.
7. `dim_staff` - Employee roles, active status, management mapping.
8. `dim_weather` - Normalized atmospheric traits.

### Fact Tables (5)
1. `fact_customer_feedback` - NPS scores, textual complaint arrays, survey flags.
2. `fact_gate_event` - Turnstile capacity entries and rejections.
3. `fact_ride_operations` - Interval operational status from Ride SCADA.
4. `fact_transaction` - POS financials (gross, net, discounts, VAT).
5. `fact_weather_observation` - Temporal climate captures mapped to operating performance.

---

## 🚀 Getting Started

### Prerequisites
- Python >= 3.11
- A Databricks workspace with an active SQL Warehouse.
- A Databricks Personal Access Token.
- (Optional but Relevent) `uv` for ultra-fast python environment management.

### Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd qiddiya-data-sql-upload/src/qiddiya_dbt
   ```

2. Establish Python Environment & Dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # (On Windows: .venv\Scripts\activate)
   pip install -r requirements.txt
   # OR using uv
   # uv sync
   ```

---

## 🕹️ Usage Guide

### Step 1: Data Generation & Bronze Upload
You need to authenticate to Databricks securely utilizing an environment variable. 

*On Linux/macOS:*
```bash
export DATABRICKS_TOKEN="your_dapi_databricks_token_here"
python qiddiya_load_databricks.py
```

*On Windows (PowerShell):*
```powershell
$env:DATABRICKS_TOKEN="your_dapi_databricks_token_here"
python qiddiya_load_databricks.py
```

*What happens next?* The script outputs real-time metrics showing phase generations, anomaly injections, referential integrity tests, and will securely sequentially insert thousands of entries utilizing SQL `INSERT INTO` batches directly into the Databricks `bronze` schema.

### Step 2: dbt Execution (Transformations)
Once the Bronze layer is fully populated, transition into the dbt project folder to run data model transformations.

1. Ensure your `~/.dbt/profiles.yml` is correctly configured identically to match `dheeraj_qiddiya_dbt_databricks`.
2. Move to the dbt project directory:
   ```bash
   cd dheeraj_qiddiya_dbt_databricks
   ```
3. Test your connection:
   ```bash
   dbt debug
   ```
4. Build all pipeline transformation logic:
   ```bash
   dbt run
   ```

---

## 🧪 Testing & Data Quality

Ensuring absolute fidelity is vital. We utilize 2 discrete testing levels:

1. **Python Ingestion Integrity Validation** 
   - Pre-deployment validation ensuring no missing lookup attributes (E.g. guaranteeing every `customer_id` present in `gate_events` exists in `customers`).
2. **dbt Generic & Singular Tests**
   - Automatically executed alongside the transform steps utilizing `dbt test`.
   - Native verification for `unique` constraints, `not_null` assertions and relationship map checking at the warehouse compiler level.
   - Core implementation of structural consistency audits (e.g. `gold_audit_reconciliation.sql`, `gold_late_arriving_dimensions.sql`).

To execute warehouse validation:
```bash
cd dheeraj_qiddiya_dbt_databricks
dbt test
```

---
*Maintained and developed by [Dheeraj Kumar].*