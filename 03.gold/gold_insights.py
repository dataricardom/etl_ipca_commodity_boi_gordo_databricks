# Databricks notebook source
# MAGIC %run "/Workspace/Projeto_Pos_Databricks/etl_ipca_commodity_boi_gordo/00.config/config"

# COMMAND ----------

from pyspark.sql import functions as f, Window

df = spark.table(f"{CATALOG}.{silver}.silver_economia")

# COMMAND ----------

df.display()

# COMMAND ----------

w = Window.orderBy('data')

# COMMAND ----------

gold_df = (df
        .withColumn('ipca_ant', f.lag('ipca').over(w))
        .withColumn('boi_ant', f.lag('boi_gordo').over(w))
        .withColumn('variacao_ipca', (f.col('ipca') - f.col('ipca_ant'))/ f.col('ipca_ant')*100)
        .withColumn('variacao_boi',(f.col('boi_gordo') - f.col('boi_ant'))/ f.col('boi_ant')*100)
        .drop('ipca_ant','boi_ant')
        )
        


# COMMAND ----------

gold_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{gold}.gold_insights")

# COMMAND ----------

gold.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kpuudata.gold_economia.gold_insights LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kpuudata.gold_economia.vw_gold_dashboard AS
# MAGIC SELECT * FROM kpuudata.gold_economia.gold_insights;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT data, variacao_ipca, variacao_boi
# MAGIC FROM kpuudata.gold_economia.vw_gold_dashboard;
