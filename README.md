# 🚗 Azure End-to-End Car Sales Data Engineering Pipeline

An end-to-end cloud-native data engineering project built on Microsoft Azure, implementing the **Medallion Architecture** (Bronze → Silver → Gold) to ingest, transform, and model car sales data for analytics and reporting.

---

## 📌 Project Overview

This project demonstrates a production-grade data pipeline that:
- Ingests raw car sales data from a GitHub-hosted CSV via **Azure Data Factory**
- Loads it into **Azure SQL Database** as the source system
- Implements **Incremental Loading** using a watermark strategy
- Stores raw data in **Azure Data Lake Storage Gen2** (Bronze layer as Parquet)
- Performs transformations using **PySpark in Azure Databricks** (Silver layer)
- Applies **Slowly Changing Dimensions (SCD Type 1)** and **Star Schema** modeling (Gold layer)
- Uses **Unity Catalog** for data governance and lineage tracking
- Orchestrates the end-to-end pipeline using **Databricks Workflows + ADF**

---

## 🏗️ Architecture

```
GitHub CSV (Source)
        │
        ▼
Azure Data Factory (source_prep pipeline)
        │   HTTP → Azure SQL Database
        ▼
Azure SQL Database (source_cars_data + watermark_table)
        │
        ▼
Azure Data Factory (Incremental pipeline)
        │   Watermark-based incremental load
        ▼
ADLS Gen2 – Bronze Layer (Parquet)
        │
        ▼
Azure Databricks – Silver Notebook (PySpark)
        │   Cleansing, deduplication, standardization
        ▼
ADLS Gen2 – Silver Layer (Parquet)
        │
        ▼
Azure Databricks – Gold Notebooks (PySpark + Delta Lake)
        │   Star Schema: Fact + Dimension tables (SCD Type 1)
        ▼
ADLS Gen2 – Gold Layer (Delta Tables)
        │
        ▼
Unity Catalog (Data Governance & Lineage)
        │
        ▼
Reporting / Analytics
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Orchestration | Azure Data Factory (ADF) |
| Processing | Azure Databricks (Premium), PySpark |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Source Database | Azure SQL Database (SQL Server 12.0) |
| Table Format | Delta Lake |
| Data Governance | Unity Catalog |
| Data Modeling | Star Schema, SCD Type 1 |
| Load Strategy | Incremental Load (Watermark) |
| File Formats | CSV (source), Parquet (Bronze/Silver), Delta (Gold) |

---

## 📦 Azure Resources

| Resource | Name | Details |
|---|---|---|
| Resource Group | `RG_AZ_Car_Project` | All project resources |
| SQL Server | `ashwarycarsaleserver` | Version 12.0, Central India |
| ADLS Gen2 | `carashwarydatalake` | HNS enabled, Standard LRS, South India |
| Databricks Workspace | `CarSalesDataBricks` | Premium SKU, South India |
| Databricks Access Connector | `Cars_Access_Connector` | System-assigned managed identity |
| ADF Pipeline 1 | `source_prep` | GitHub CSV → Azure SQL |
| ADF Pipeline 2 | `Incremental` | Azure SQL → Bronze (ADLS Gen2) |

---

## 📁 Project Structure

```
azure-car-sales-pipeline/
│
├── adf_pipelines/
│   ├── source_prep.json          # ADF pipeline: GitHub → Azure SQL
│   └── incremental_load.json     # ADF pipeline: Incremental SQL → Bronze
│
├── databricks_notebooks/
│   ├── silver_notebook.py        # Bronze → Silver transformation
│   ├── gold_dim_branch.py        # Dimension: Branch
│   ├── gold_dim_dealer.py        # Dimension: Dealer
│   ├── gold_dim_model.py         # Dimension: Car Model (SCD Type 1)
│   ├── gold_dim_date.py          # Dimension: Date
│   └── gold_fact_sales.py        # Fact Table: Car Sales
│
├── sql/
│   ├── create_source_table.sql   # Source table schema
│   ├── create_watermark_table.sql
│   └── update_watermark_sp.sql   # Stored procedure
│
├── architecture/
│   └── architecture_diagram.png
│
└── README.md
```

---

## 🔄 Pipeline Details

### Pipeline 1: Source Preparation (`source_prep`)
- **Source:** CSV file hosted on GitHub (`SalesData.csv`), fetched via HTTP connector
- **Sink:** Azure SQL Database table `source_cars_data`
- **Key features:**
  - Parameterized file name for dynamic loading
  - Column mapping with type casting (e.g., Revenue → `bigint`, Date_ID → `varchar`)
  - Fields: `Branch_ID`, `Dealer_ID`, `Model_ID`, `Revenue`, `Units_Sold`, `Date_Id`, `Day`, `Month`, `Year`, `Branch_Name`, `Dealer_Name`

### Pipeline 2: Incremental Load (`Incremental`)
- **Source:** Azure SQL Database (`source_cars_data`)
- **Sink:** ADLS Gen2 Bronze container (Parquet format)
- **Activities:**
  1. `last_load_lookup` — reads `watermark_table` to get last processed date
  2. `current_load_lookup` — gets `MAX(Date_Id)` from source table
  3. `Copy_increm_data` — copies only new records using date filter:
     ```sql
     SELECT * FROM source_cars_data
     WHERE Date_Id > '<last_load>'
       AND Date_Id <= '<current_max_date>'
     ```
  4. `Stored procedure Watermark` — calls `[dbo].[UpdateWatermarkTable]` to update watermark

---

## 🧱 Medallion Architecture

### 🥉 Bronze Layer
- Raw data stored as **Parquet** files in ADLS Gen2
- No transformation — exact replica of source SQL data
- Schema: `Branch_ID`, `Dealer_ID`, `Model_ID`, `Revenue`, `Units_Sold`, `Date_Id`, `Day`, `Month`, `Year`, `Branch_Name`, `Dealer_Name`

### 🥈 Silver Layer
- Cleaned and standardized data using **PySpark in Databricks**
- Operations: deduplication, null handling, data type standardization
- Output: Parquet files in Silver container

### 🥇 Gold Layer
- **Star Schema** modeled using **Delta Tables**
- Governed by **Unity Catalog**

#### Dimension Tables (SCD Type 1)
| Table | Key | Description |
|---|---|---|
| `dim_branch` | `dim_branch_key` | Branch name and ID |
| `dim_dealer` | `dim_dealer_key` | Dealer name and ID |
| `dim_model` | `dim_model_key` | Car model and category |
| `dim_date` | `dim_date_key` | Date, Day, Month, Year |

#### Fact Table
| Table | Description |
|---|---|
| `fact_sales` | Revenue, Units Sold, with FK references to all dimensions |

---

## 🔐 Security & Governance

- **Managed Identity:** Databricks Access Connector uses System-Assigned Managed Identity to securely access ADLS Gen2 without credentials
- **Unity Catalog:** Enforces schema-level governance, data lineage, and role-based access control (RBAC)
- **TLS 1.2:** Enforced on SQL Server
- **No public blob access** on ADLS Gen2
- **Azure AD Authentication** configured on SQL Server

---

## 📊 Dataset

**Car Sales Dataset** — sourced from GitHub

| Column | Type | Description |
|---|---|---|
| Branch_ID | varchar | Unique branch identifier |
| Dealer_ID | varchar | Unique dealer identifier |
| Model_ID | varchar | Car model identifier |
| Revenue | bigint | Sales revenue |
| Units_Sold | bigint | Number of cars sold |
| Date_Id | varchar | Sale date (used for incremental load watermark) |
| Day | int | Day of sale |
| Month | int | Month of sale |
| Year | int | Year of sale |
| Branch_Name | varchar | Name of the branch |
| Dealer_Name | varchar | Name of the dealer |

---

## 🚀 How to Reproduce

1. **Set up Azure resources:** Create Resource Group, SQL Server, ADLS Gen2 (with HNS enabled), Databricks workspace (Premium), and Databricks Access Connector
2. **Configure ADLS Gen2:** Create containers — `bronze`, `silver`, `gold`
3. **Assign IAM roles:** Grant Databricks Access Connector the `Storage Blob Data Contributor` role on ADLS Gen2
4. **Set up SQL Database:** Create `source_cars_data` table and `watermark_table`, deploy the `UpdateWatermarkTable` stored procedure
5. **Configure ADF:** Create linked services for GitHub (HTTP) and Azure SQL, deploy both pipeline JSONs
6. **Run source_prep pipeline:** Loads full dataset into SQL
7. **Run Incremental pipeline:** Loads new records into Bronze layer
8. **Set up Databricks Unity Catalog:** Configure metastore and catalog
9. **Run Silver notebook:** Transforms Bronze → Silver
10. **Run Gold notebooks:** Builds dimension and fact tables
11. **Configure Databricks Workflow:** Orchestrate Silver + Gold notebooks end-to-end

---

## 🔗 References

- **Tutorial:** [Azure End-To-End Data Engineering Project by Ansh Lamba](https://www.youtube.com/watch?v=6_hXeNg9TJ0)
- **Data Source:** [GitHub – Car Sales Dataset](https://github.com/anshlambagit)

---

## 👩‍💻 Author

**Neha Yadav**  
Data Engineer | Azure | Databricks | PySpark  
📎 [LinkedIn](https://linkedin.com/in/nehayadav26) | 💻 [GitHub](https://github.com/NehaYadav1997)
