# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Setup do ambiente
# MAGIC Cria o catálogo, os esquemas das camadas (bronze, silver, gold) e o volume onde o CSV bruto será enviado.
# MAGIC **Rode este notebook uma única vez, antes de enviar o arquivo.**

# COMMAND ----------

CATALOG = "mvp_hotel"
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print("Não foi possível criar o catálogo; usando o catálogo 'workspace'.\n", e)
    CATALOG = "workspace"

for camada in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{camada}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw")

print(f"Catálogo usado: {CATALOG}")
print(f"Agora envie hotel_bookings.csv para: /Volumes/{CATALOG}/bronze/raw/")
