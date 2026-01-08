# Databricks notebook source
#Importando configurações de catálogo e schema:
%run "/Workspace/Projeto_Pos_Databricks/etl_ipca_commodity_boi_gordo/00.config/config"

# COMMAND ----------

#Importando bibliotecas nescessárias:
from pyspark.sql import functions as f
from pyspark.sql.functions import to_date, col, greatest, regexp_replace, date_format

# COMMAND ----------

#Atribuindo tabelas bronze ao dataframe:
ipca = spark.table(f"{CATALOG}.{bronze}.bronze_ipca")
boi_gordo = spark.table(f"{CATALOG}.{bronze}.bronze_boi_gordo")

# COMMAND ----------

#Renomeando Colunas Boi Gordo:
boi_gordo = (
    boi_gordo
    .withColumnRenamed('Data', 'data')
    .withColumnRenamed('Valor','boi_gordo')
)

boi_gordo.display()
boi_gordo.printSchema()

# COMMAND ----------

boi_gordo = boi_gordo.withColumn('data', to_date('data','MM/yyyy')) #1 Transformando os dados para o formato de data.
boi_gordo = boi_gordo.withColumn('data', date_format('data', 'yyyy-MM'))#2 Transformando os dados para formato string.

boi_gordo.display()
boi_gordo.printSchema()

# COMMAND ----------

#Alterando "," por "." e convertendo coluna de string para float:
boi_gordo = boi_gordo.withColumn('boi_gordo', regexp_replace(col('boi_gordo'), ',', '.').cast('float'))

boi_gordo.display()
boi_gordo.printSchema()

# COMMAND ----------

ipca = ipca.withColumn('data', to_date('data', 'dd/MM/yyyy')) #1 Transformando os dados para o formato de data.




# COMMAND ----------

ipca = ipca.withColumn('data', date_format('data', 'yyyy-MM')) #2 Transformando os dados para formato string.

# COMMAND ----------

ipca.display()


# COMMAND ----------

#Realizando o primeiro join entre as tabelas sem uso de alias:
df_join = (
    ipca.join(boi_gordo, on= 'data', how='inner')
        .select('data', 'ipca', 'boi_gordo')
)

df_join.display()

# COMMAND ----------

#Realizando o mesmo join, agora utilizando de alias e adicionando a coluna data_coleta:
ip = ipca.alias('ip')
bo = boi_gordo.alias('bo')

df_join = (
    ip.join(bo, col('ip.data') == col('bo.data'), 'inner')
        .select(
            col('ip.data').alias('data'),
            col('ip.ipca').alias('ipca'),
            col('bo.boi_gordo').alias('boi_gordo'),
            col('ip.data_coleta').alias('data_coleta')
        )
)

df_join.display()

# COMMAND ----------

#Transformando coluna do join em data novamente:
df_join = df_join.withColumn('data', to_date('data', 'yyyy-MM'))

# COMMAND ----------

#Convertendo o tipo de dados das colunas ipca e boi_gordo para double:
df_join = (
    df_join
    .withColumn('ipca', col('ipca').cast('double'))
    .withColumn('boi_gordo', col('boi_gordo').cast('double'))
    
)

df_join.display()

# COMMAND ----------

df_join.orderBy('data').limit(50).display()

# COMMAND ----------

#Salvando o dataframe como uma tabela delta no schema silver_economia com o nome silver_economia:
df_join.write.format('delta').mode('overwrite').saveAsTable(f"{CATALOG}.{silver}.silver_economia")
