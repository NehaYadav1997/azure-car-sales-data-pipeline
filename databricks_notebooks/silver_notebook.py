# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation Notebook
# MAGIC **Pipeline:** Bronze → Silver  
# MAGIC **Purpose:** Cleanse, deduplicate, and standardize raw car sales data from Bronze (Parquet) and write to Silver layer  
# MAGIC **Format:** Parquet  
# MAGIC **Governed by:** Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure Access to ADLS Gen2 via Unity Catalog / Access Connector
# MAGIC Access is granted through the Databricks Access Connector (Managed Identity) assigned the
# MAGIC Storage Blob Data Contributor role on ADLS Gen2. No credentials are stored in the notebook.

# COMMAND ----------

# Storage account details
storage_account_name = "carashwarydatalake"
bronze_container = "bronze"
silver_container = "silver"

bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"

print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Layer (Parquet)

# COMMAND ----------

df_bronze = spark.read.format("parquet").load(bronze_path)

print(f"Record count: {df_bronze.count()}")
print(f"Schema:")
df_bronze.printSchema()
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Cleansing & Standardization

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType

# ----- 3a. Drop duplicate records -----
df_deduped = df_bronze.dropDuplicates()
print(f"After deduplication: {df_deduped.count()} records (removed {df_bronze.count() - df_deduped.count()} duplicates)")

# COMMAND ----------

# ----- 3b. Drop rows where critical fields are null -----
critical_cols = ["Branch_ID", "Dealer_ID", "Model_ID", "Date_Id", "Revenue", "Units_Sold"]

df_clean = df_deduped.dropna(subset=critical_cols)
print(f"After dropping nulls in critical columns: {df_clean.count()} records")

# COMMAND ----------

# ----- 3c. Ensure correct data types -----
df_typed = (
    df_clean
    .withColumn("Revenue",    F.col("Revenue").cast(LongType()))
    .withColumn("Units_Sold", F.col("Units_Sold").cast(LongType()))
    .withColumn("Day",        F.col("Day").cast(IntegerType()))
    .withColumn("Month",      F.col("Month").cast(IntegerType()))
    .withColumn("Year",       F.col("Year").cast(IntegerType()))
    .withColumn("Branch_ID",  F.trim(F.col("Branch_ID")))
    .withColumn("Dealer_ID",  F.trim(F.col("Dealer_ID")))
    .withColumn("Model_ID",   F.trim(F.col("Model_ID")))
    .withColumn("Branch_Name",F.trim(F.col("Branch_Name")))
    .withColumn("Dealer_Name",F.trim(F.col("Dealer_Name")))
    .withColumn("Date_Id",    F.trim(F.col("Date_Id")))
)

# COMMAND ----------

# ----- 3d. Extract model category from Model_ID (e.g., "Hon-M69" → "Hon") -----
df_silver = df_typed.withColumn(
    "Model_Category",
    F.split(F.col("Model_ID"), "-")[0]
)

# COMMAND ----------

# ----- 3e. Add ingestion metadata -----
df_silver = df_silver.withColumn("ingestion_date", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview Silver Data

# COMMAND ----------

print(f"Final silver record count: {df_silver.count()}")
df_silver.printSchema()
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Silver Layer (Parquet, overwrite)

# COMMAND ----------

(
    df_silver
    .write
    .format("parquet")
    .mode("overwrite")
    .save(silver_path)
)

print(f"✅ Silver layer written successfully to: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Write

# COMMAND ----------

df_verify = spark.read.format("parquet").load(silver_path)
print(f"Verified silver record count: {df_verify.count()}")
df_verify.display()
