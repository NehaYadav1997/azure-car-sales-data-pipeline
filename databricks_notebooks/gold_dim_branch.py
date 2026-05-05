# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Dimension: dim_branch
# MAGIC **Pipeline:** Silver → Gold (Dimension Table)  
# MAGIC **Strategy:** Slowly Changing Dimension Type 1 (SCD1)  
# MAGIC **Format:** Delta Table | **Catalog:** Unity Catalog

# COMMAND ----------

storage_account_name = "carashwarydatalake"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_branch"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df_silver = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract Distinct Branches (Source)

# COMMAND ----------

df_src = (
    df_silver
    .select("Branch_ID", "Branch_Name")
    .distinct()
)

print(f"Distinct branches in source: {df_src.count()}")
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SCD Type 1 – Initial or Incremental Load

# COMMAND ----------

def tableExists(path):
    try:
        spark.read.format("delta").load(path)
        return True
    except:
        return False

# COMMAND ----------

if not tableExists(gold_path):
    # Initial load — write empty schema first, then append with surrogate keys
    df_empty = df_src.where("1=0").withColumn("dim_branch_key", F.lit(None).cast("long"))
    df_empty.write.format("delta").mode("overwrite").save(gold_path)

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df_new = df_src.withColumn("dim_branch_key", F.row_number().over(window_spec))

    df_new.write.format("delta").mode("append").save(gold_path)
    print(f"✅ Initial load: {df_new.count()} branches written.")

else:
    # Incremental load — insert only new Branch_IDs
    df_sink = spark.read.format("delta").load(gold_path)
    df_new_records = (
        df_src.join(df_sink.select("Branch_ID"), on="Branch_ID", how="left_anti")
    )
    new_count = df_new_records.count()
    print(f"New branch records to insert: {new_count}")

    if new_count > 0:
        max_key = df_sink.agg(F.max("dim_branch_key")).collect()[0][0] or 0
        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_to_insert = df_new_records.withColumn(
            "dim_branch_key", F.row_number().over(window_spec) + max_key
        )
        DeltaTable.forPath(spark, gold_path).alias("target").merge(
            df_to_insert.alias("source"),
            "target.Branch_ID = source.Branch_ID"
        ).whenNotMatchedInsertAll().execute()
        print(f"✅ Incremental load: {new_count} new branches inserted.")
    else:
        print("✅ dim_branch is already up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify

# COMMAND ----------

df_gold = spark.read.format("delta").load(gold_path)
print(f"Total records in dim_branch: {df_gold.count()}")
df_gold.orderBy("dim_branch_key").display()
