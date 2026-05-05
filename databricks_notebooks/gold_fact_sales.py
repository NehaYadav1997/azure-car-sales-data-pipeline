# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Fact Table: fact_sales
# MAGIC **Pipeline:** Silver + Dimensions → Gold Fact Table  
# MAGIC **Format:** Delta Table  
# MAGIC **Catalog:** Unity Catalog  
# MAGIC **Model:** Star Schema — joins all dimension surrogate keys

# COMMAND ----------

storage_account_name = "carashwarydatalake"
silver_path     = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
gold_base       = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"

dim_branch_path = gold_base + "dim_branch"
dim_dealer_path = gold_base + "dim_dealer"
dim_model_path  = gold_base + "dim_model"
dim_date_path   = gold_base + "dim_date"
fact_sales_path = gold_base + "fact_sales"

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Silver and All Dimensions

# COMMAND ----------

df_silver     = spark.read.format("parquet").load(silver_path)
dim_branch    = spark.read.format("delta").load(dim_branch_path)
dim_dealer    = spark.read.format("delta").load(dim_dealer_path)
dim_model     = spark.read.format("delta").load(dim_model_path)
dim_date      = spark.read.format("delta").load(dim_date_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Fact Table by Joining Silver with Dimension Surrogate Keys

# COMMAND ----------

df_fact = (
    df_silver
    # Join Branch dimension
    .join(
        dim_branch.select("Branch_ID", "dim_branch_key"),
        on="Branch_ID",
        how="left"
    )
    # Join Dealer dimension
    .join(
        dim_dealer.select("Dealer_ID", "dim_dealer_key"),
        on="Dealer_ID",
        how="left"
    )
    # Join Model dimension
    .join(
        dim_model.select("Model_ID", "dim_model_key"),
        on="Model_ID",
        how="left"
    )
    # Join Date dimension
    .join(
        dim_date.select("Date_Id", "dim_date_key"),
        on="Date_Id",
        how="left"
    )
    # Select only fact columns (surrogate keys + measures)
    .select(
        "dim_branch_key",
        "dim_dealer_key",
        "dim_model_key",
        "dim_date_key",
        "Revenue",
        "Units_Sold",
        F.current_timestamp().alias("load_timestamp")
    )
)

print(f"Fact table record count: {df_fact.count()}")
df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Fact Table to Gold (Delta, Overwrite)

# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .save(fact_sales_path)
)

print(f"✅ fact_sales written successfully to: {fact_sales_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify

# COMMAND ----------

df_verify = spark.read.format("delta").load(fact_sales_path)
print(f"Verified fact_sales count: {df_verify.count()}")
df_verify.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sample Revenue Analysis

# COMMAND ----------

# Total revenue by model
df_verify.groupBy("dim_model_key").agg(
    F.sum("Revenue").alias("Total_Revenue"),
    F.sum("Units_Sold").alias("Total_Units_Sold")
).orderBy(F.desc("Total_Revenue")).display()
