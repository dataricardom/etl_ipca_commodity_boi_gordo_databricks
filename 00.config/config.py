# Databricks notebook source

CATALOG = 'kpuudata' #Definindo Catálogo

spark.sql(f"USE CATALOG {CATALOG}") #Configurando para usar o catálogo.

#Definindo Schemas
bronze = "bronze_economia"
silver = "silver_economia"
gold = "gold_economia"

#Criando Schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{gold}")

print("Catálogo utilizado:", CATALOG)
print("Schemas criados:", bronze, silver, gold)
