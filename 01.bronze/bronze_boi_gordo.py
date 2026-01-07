# Databricks notebook source
# MAGIC %run "/Workspace/Projeto_Pos_Databricks/etl_ipca_commodity_boi_gordo/00.config/config"

# COMMAND ----------

df = spark.table("kpuudata.default.boi_gordo")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn("data_coleta", current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{bronze}.bronze_boi_gordo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kpuudata.bronze_economia.bronze_boi_gordo;
