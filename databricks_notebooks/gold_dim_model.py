# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Dimension: dim_model
# MAGIC **Pipeline:** Silver → Gold (Dimension Table)  
# MAGIC **Strategy:** Slowly Changing Dimension Type 1 (SCD1) — overwrite on change  
# MAGIC **Format:** Delta Table  
# MAGIC **Catalog:** Unity Catalog

# COMMAND ----------

storage_account_name = "carashwarydatalake"
silver_container = "silver"
gold_container   = "gold"

silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/dim_model"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Silver Layer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df_silver = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Distinct Models (Source)

# COMMAND ----------

df_src = (
    df_silver
    .select("Model_ID", "Model_Category")
    .distinct()
)

print(f"Distinct models in source: {df_src.count()}")
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 1 Logic with Surrogate Key

# COMMAND ----------

table_name = "dim_model"

def tableExists(table_path):
    try:
        spark.read.format("delta").load(table_path)
        return True
    except:
        return False

# COMMAND ----------

if not tableExists(gold_path):
    # MAGIC %md ### Initial Load — create schema only (no data), then load with keys starting at 1
    
    # Empty schema creation
    df_empty = df_src.where("1=0").withColumn("dim_model_key", F.lit(None).cast("long"))
    (
        df_empty
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_path)
    )
    
    # Load actual data with surrogate keys starting at 1
    df_sink = spark.read.format("delta").load(gold_path)
    
    incremental_flag = "0"
    
    # Left join to find new records (all records are new on first load)
    df_joined = df_src.join(df_sink, on="Model_ID", how="left")
    df_filter = df_joined.filter(F.col("dim_model_key").isNull()).select(
        df_src["Model_ID"],
        df_src["Model_Category"]
    )
    
    # Assign surrogate keys starting at 1
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df_new = df_filter.withColumn(
        "dim_model_key",
        F.row_number().over(window_spec)
    )
    
    (
        df_new
        .write
        .format("delta")
        .mode("append")
        .save(gold_path)
    )
    print(f"✅ Initial load complete. {df_new.count()} records written to {table_name}.")

else:
    # MAGIC %md ### Incremental Load — find new records, assign next available surrogate keys
    
    df_sink = spark.read.format("delta").load(gold_path)
    
    # Find records in source not yet in dimension
    df_joined = df_src.join(df_sink, on="Model_ID", how="left")
    df_new_records = df_joined.filter(F.col("dim_model_key").isNull()).select(
        df_src["Model_ID"],
        df_src["Model_Category"]
    )
    
    new_count = df_new_records.count()
    print(f"New model records to insert: {new_count}")
    
    if new_count > 0:
        # Get the current max surrogate key
        max_key = df_sink.agg(F.max("dim_model_key")).collect()[0][0] or 0
        
        # Assign next keys
        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_to_insert = df_new_records.withColumn(
            "dim_model_key",
            F.row_number().over(window_spec) + max_key
        )
        
        # Upsert using Delta merge
        delta_table = DeltaTable.forPath(spark, gold_path)
        (
            delta_table.alias("target")
            .merge(
                df_to_insert.alias("source"),
                "target.Model_ID = source.Model_ID"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"✅ Incremental load complete. {new_count} new records inserted into {table_name}.")
    else:
        print("✅ No new records to insert. dim_model is up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Gold dim_model

# COMMAND ----------

df_gold = spark.read.format("delta").load(gold_path)
print(f"Total records in dim_model: {df_gold.count()}")
df_gold.orderBy("dim_model_key").display()
