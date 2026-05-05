# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Dimension: dim_date
# MAGIC **Pipeline:** Silver → Gold (Dimension Table)  
# MAGIC **Strategy:** Slowly Changing Dimension Type 1 (SCD1)  
# MAGIC **Format:** Delta Table | **Catalog:** Unity Catalog

# COMMAND ----------

storage_account_name = "carashwarydatalake"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_date"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df_silver = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract Distinct Dates (Source)

# COMMAND ----------

df_src = (
    df_silver
    .select("Date_Id", "Day", "Month", "Year")
    .distinct()
)

print(f"Distinct date records in source: {df_src.count()}")
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
    df_empty = df_src.where("1=0").withColumn("dim_date_key", F.lit(None).cast("long"))
    df_empty.write.format("delta").mode("overwrite").save(gold_path)

    window_spec = Window.orderBy("Year", "Month", "Day")
    df_new = df_src.withColumn("dim_date_key", F.row_number().over(window_spec))

    df_new.write.format("delta").mode("append").save(gold_path)
    print(f"✅ Initial load: {df_new.count()} date records written.")

else:
    df_sink = spark.read.format("delta").load(gold_path)
    df_new_records = (
        df_src.join(df_sink.select("Date_Id"), on="Date_Id", how="left_anti")
    )
    new_count = df_new_records.count()
    print(f"New date records to insert: {new_count}")

    if new_count > 0:
        max_key = df_sink.agg(F.max("dim_date_key")).collect()[0][0] or 0
        window_spec = Window.orderBy("Year", "Month", "Day")
        df_to_insert = df_new_records.withColumn(
            "dim_date_key", F.row_number().over(window_spec) + max_key
        )
        DeltaTable.forPath(spark, gold_path).alias("target").merge(
            df_to_insert.alias("source"),
            "target.Date_Id = source.Date_Id"
        ).whenNotMatchedInsertAll().execute()
        print(f"✅ Incremental load: {new_count} new date records inserted.")
    else:
        print("✅ dim_date is already up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify

# COMMAND ----------

df_gold = spark.read.format("delta").load(gold_path)
print(f"Total records in dim_date: {df_gold.count()}")
df_gold.orderBy("dim_date_key").display()
