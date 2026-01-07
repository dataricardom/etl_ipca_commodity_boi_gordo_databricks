# Databricks notebook source
#Importando configurações de catalogo e schema:
%run "/Workspace/Projeto_Pos_Databricks/etl_ipca_commodity_boi_gordo/00.config/config"

# COMMAND ----------

#Importando bibliotecas nescessárias:
import requests
import pandas as pd
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#Capturando dados da API
url="https://api.bcb.gov.br/dados/serie/bcdata.sgs.10844/dados" #Definindo url
params={
    "formato":"json",
    "dataInicial":"01/01/2025",
    "dataFinal":"31/12/2025"
} #Definindo parâmetros

#Criando Dataframe a partir dos dados da api.
df_pd = pd.DataFrame(requests.get(url, params=params).json())
df_pd.columns = ["data","ipca"]
df_pd["ipca"] = df_pd["ipca"].str.replace(",",".").astype(float) #Alterando valores separados por "," para "." e convertendo em float.


# COMMAND ----------

df_pd.display()

# COMMAND ----------

df_pd = spark.createDataFrame(df_pd).withColumn("data_coleta", current_timestamp()) #Criando um spark dataframe e adicionando coluna com data_coleta

# COMMAND ----------

df_pd.display()

# COMMAND ----------

#Salvando dados no delta no catálogo e schema definidos.
df_pd.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{bronze}.bronze_ipca")
