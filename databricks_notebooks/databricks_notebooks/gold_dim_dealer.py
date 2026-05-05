# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Dimension: dim_dealer
# MAGIC **Pipeline:** Silver → Gold (Dimension Table)  
# MAGIC **Strategy:** Slowly Changing Dimension Type 1 (SCD1)  
# MAGIC **Format:** Delta Table | **Catalog:** Unity Catalog

# COMMAND ----------

storage_account_name = "carashwarydatalake"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_dealer"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df_silver = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract Distinct Dealers (Source)

# COMMAND ----------

df_src = (
    df_silver
    .select("Dealer_ID", "Dealer_Name")
    .distinct()
)

print(f"Distinct dealers in source: {df_src.count()}")
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
    df_empty = df_src.where("1=0").withColumn("dim_dealer_key", F.lit(None).cast("long"))
    df_empty.write.format("delta").mode("overwrite").save(gold_path)

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df_new = df_src.withColumn("dim_dealer_key", F.row_number().over(window_spec))

    df_new.write.format("delta").mode("append").save(gold_path)
    print(f"✅ Initial load: {df_new.count()} dealers written.")

else:
    df_sink = spark.read.format("delta").load(gold_path)
    df_new_records = (
        df_src.join(df_sink.select("Dealer_ID"), on="Dealer_ID", how="left_anti")
    )
    new_count = df_new_records.count()
    print(f"New dealer records to insert: {new_count}")

    if new_count > 0:
        max_key = df_sink.agg(F.max("dim_dealer_key")).collect()[0][0] or 0
        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_to_insert = df_new_records.withColumn(
            "dim_dealer_key", F.row_number().over(window_spec) + max_key
        )
        DeltaTable.forPath(spark, gold_path).alias("target").merge(
            df_to_insert.alias("source"),
            "target.Dealer_ID = source.Dealer_ID"
        ).whenNotMatchedInsertAll().execute()
        print(f"✅ Incremental load: {new_count} new dealers inserted.")
    else:
        print("✅ dim_dealer is already up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify

# COMMAND ----------

df_gold = spark.read.format("delta").load(gold_path)
print(f"Total records in dim_dealer: {df_gold.count()}")
df_gold.orderBy("dim_dealer_key").display()
