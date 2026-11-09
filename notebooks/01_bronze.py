# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Camada Bronze
# MAGIC Lê o CSV original **sem alterar o conteúdo** (todas as colunas como texto) e grava como tabela Delta,
# MAGIC acrescentando colunas de linhagem: arquivo de origem e data/hora da ingestão.

# COMMAND ----------

dbutils.widgets.text("catalog", "mvp_hotel")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

caminho = f"/Volumes/{CATALOG}/bronze/raw/hotel_bookings.csv"

# Volume do arquivo original (para a seção de coleta do paper)
info = [f for f in dbutils.fs.ls(f"/Volumes/{CATALOG}/bronze/raw/") if f.name == "hotel_bookings.csv"][0]
print(f"Tamanho do arquivo: {info.size / 1024 / 1024:.1f} MB")

# COMMAND ----------

bronze = (spark.read
          .option("header", True)
          .option("inferSchema", False)      # bronze = dado bruto, tudo como texto
          .csv(caminho)
          .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("_data_ingestao", F.current_timestamp()))

(bronze.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.bronze.hotel_bookings"))

t = spark.table(f"{CATALOG}.bronze.hotel_bookings")
print(f"Registros na Bronze: {t.count():,}  |  Colunas: {len(t.columns)}")
display(t.limit(10))
