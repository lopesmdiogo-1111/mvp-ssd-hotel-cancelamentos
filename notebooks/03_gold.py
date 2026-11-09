# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Camada Gold: esquema estrela
# MAGIC Dimensões derivadas por projeção dos valores distintos, com chaves substitutas, e tabela fato no grão de **uma reserva**.

# COMMAND ----------

dbutils.widgets.text("catalog", "mvp_hotel")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sv = spark.table(f"{CATALOG}.silver.reservas")

def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.{nome}")
    print(f"{nome}: {spark.table(f'{CATALOG}.gold.{nome}').count():,} linhas")

def dimensao(cols, sk, nome):
    d = sv.select(*cols).distinct()
    d = d.withColumn(sk, F.row_number().over(Window.orderBy(*cols))).select(sk, *cols)
    salvar(d, nome)
    return spark.table(f"{CATALOG}.gold.{nome}")

dim_hotel  = dimensao(["tipo_hotel"], "sk_hotel", "dim_hotel")
dim_canal  = dimensao(["segmento", "canal_distrib", "tipo_deposito", "agente"], "sk_canal", "dim_canal")
dim_perfil = dimensao(["pais", "tipo_cliente", "recorrente", "faixa_cancel_prev"], "sk_perfil", "dim_perfil_cliente")

# COMMAND ----------

dim_tempo = (sv.select(F.col("data_chegada").alias("data")).distinct()
    .select(F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
            "data",
            F.year("data").alias("ano"),
            F.month("data").alias("mes"),
            F.weekofyear("data").alias("semana"),
            F.when(F.month("data").isin(6, 7, 8), "alta")
             .when(F.month("data").isin(12, 1, 2), "baixa").otherwise("media").alias("temporada")))
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

fato = (sv
    .join(dim_hotel,  "tipo_hotel")
    .join(dim_canal,  ["segmento", "canal_distrib", "tipo_deposito", "agente"])
    .join(dim_perfil, ["pais", "tipo_cliente", "recorrente", "faixa_cancel_prev"])
    .select("id_reserva", "sk_hotel",
            F.date_format("data_chegada", "yyyyMMdd").cast("int").alias("sk_data_chegada"),
            "sk_canal",
            F.col("sk_perfil").alias("sk_perfil_cliente"),
            "flag_cancelada", "lead_time", "adr", "noites_total", "hospedes",
            "receita_potencial", "alteracoes", "dias_cancel_chegada"))
salvar(fato, "fato_reserva")

# Conferência de integridade: nenhuma reserva pode se perder nos joins
assert spark.table(f"{CATALOG}.gold.fato_reserva").count() == sv.count(), "Perda de linhas na fato!"
print("OK: fato com o mesmo número de reservas da Silver.")
