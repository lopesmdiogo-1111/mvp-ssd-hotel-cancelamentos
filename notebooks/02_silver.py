# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Camada Silver: qualidade de dados e transformação
# MAGIC 1. **Perfil de qualidade** da Bronze (números da Tabela 3 do paper).
# MAGIC 2. **Regras de negócio**: tipagem, nulos, rejeitados, padronizações e colunas derivadas.

# COMMAND ----------

dbutils.widgets.text("catalog", "mvp_hotel")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

b = spark.table(f"{CATALOG}.bronze.hotel_bookings")
colunas = [c for c in b.columns if not c.startswith("_")]
total = b.count()
print(f"Total de registros na Bronze: {total:,}")

# COMMAND ----------

# MAGIC %md ## 1. Perfil de qualidade (antes dos tratamentos)

# COMMAND ----------

# Completude: nulos reais + textos "NULL"/"NA"/vazio, por atributo
contagem = b.select([
    F.sum(F.when(F.col(c).isNull() | F.trim(F.col(c)).isin("NULL", "NA", ""), 1).otherwise(0)).alias(c)
    for c in colunas
]).first().asDict()
completude = spark.createDataFrame(
    [(c, int(n), round(100 * n / total, 2)) for c, n in contagem.items()],
    ["atributo", "ausentes", "pct_ausentes"]).orderBy(F.desc("ausentes"))
display(completude)

# COMMAND ----------

# Unicidade: linhas totalmente idênticas
distintas = b.select(colunas).distinct().count()
print(f"Linhas idênticas (excedentes): {total - distintas:,}")

# Acurácia: hóspedes zero e diária (adr) anômala
tmp = b.select(
    (F.expr("try_cast(adults AS INT)") + F.coalesce(F.expr("try_cast(children AS INT)"), F.lit(0))
     + F.expr("try_cast(babies AS INT)")).alias("hospedes"),
    F.expr("try_cast(adr AS DOUBLE)").alias("adr"))
print("Reservas com zero hóspedes:", tmp.filter("hospedes = 0").count())
print("Diárias negativas:", tmp.filter("adr < 0").count())
print("Diárias acima de 1000 EUR:", tmp.filter("adr > 1000").count())

# Conformidade: categorias de refeição
display(b.groupBy("meal").count().orderBy("meal"))

# Consistência: is_canceled x reservation_status
display(b.groupBy("is_canceled", "reservation_status").count().orderBy("is_canceled"))

# COMMAND ----------

# MAGIC %md ## 2. Transformações (regras de negócio i–viii do paper)

# COMMAND ----------

inteiros = ["is_canceled", "lead_time", "arrival_date_year", "arrival_date_week_number",
            "arrival_date_day_of_month", "stays_in_weekend_nights", "stays_in_week_nights",
            "adults", "children", "babies", "is_repeated_guest", "previous_cancellations",
            "previous_bookings_not_canceled", "booking_changes", "days_in_waiting_list",
            "required_car_parking_spaces", "total_of_special_requests"]

s = b
for c in inteiros:
    s = s.withColumn(c, F.expr(f"try_cast({c} AS INT)"))
s = s.withColumn("adr", F.expr("try_cast(adr AS DOUBLE)"))

# (ii) textos "NULL" -> nulos reais -> rótulos
for c in ["agent", "country", "company"]:
    s = s.withColumn(c, F.when(F.trim(F.col(c)).isin("NULL", "NA", ""), None).otherwise(F.trim(F.col(c))))

# (iii) children nulo -> 0
s = s.withColumn("children", F.coalesce("children", F.lit(0)))

# (i) data de chegada a partir de ano + nome do mês + dia
meses = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7,
         "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
mapa = F.create_map(*[F.lit(x) for par in meses.items() for x in par])
s = s.withColumn("data_chegada", F.make_date("arrival_date_year",
                                             F.element_at(mapa, F.col("arrival_date_month")),
                                             "arrival_date_day_of_month"))
s = s.withColumn("data_status", F.to_date(F.expr("try_to_timestamp(reservation_status_date, 'yyyy-MM-dd')")))

s = (s.withColumn("hospedes", F.col("adults") + F.col("children") + F.col("babies"))
      .withColumn("noites_total", F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights")))

# (iv) rejeitados
s = s.withColumn("motivo_rejeicao",
        F.when(F.col("hospedes") == 0, "sem_hospedes")
         .when(F.col("adr") < 0, "adr_negativa")
         .when(F.col("adr") > 1000, "adr_implausivel")
         .when(F.col("data_chegada").isNull(), "data_chegada_invalida"))

rejeitados = s.filter(F.col("motivo_rejeicao").isNotNull())
ok = s.filter(F.col("motivo_rejeicao").isNull())

# (v), (vi), (vii), (viii) e renomeação para o modelo
silver = ok.select(
    F.monotonically_increasing_id().alias("id_reserva"),
    F.col("hotel").alias("tipo_hotel"),
    (F.col("is_canceled") == 1).alias("flag_cancelada"),
    "lead_time",
    "data_chegada",
    F.col("market_segment").alias("segmento"),
    F.col("distribution_channel").alias("canal_distrib"),
    F.col("deposit_type").alias("tipo_deposito"),
    F.coalesce("agent", F.lit("sem_agente")).alias("agente"),
    F.coalesce("country", F.lit("desconhecido")).alias("pais"),
    F.col("customer_type").alias("tipo_cliente"),
    (F.col("is_repeated_guest") == 1).alias("recorrente"),
    F.col("previous_cancellations").alias("cancel_anteriores"),
    F.when(F.col("previous_cancellations") == 0, "0")
     .when(F.col("previous_cancellations") == 1, "1").otherwise("2+").alias("faixa_cancel_prev"),
    F.round("adr", 2).alias("adr"),
    "noites_total",
    "hospedes",
    F.round(F.col("adr") * F.col("noites_total"), 2).alias("receita_potencial"),
    F.col("booking_changes").alias("alteracoes"),
    F.when(F.col("meal").isin("Undefined", "SC"), "SC").otherwise(F.col("meal")).alias("refeicao"),
    F.col("reservation_status").alias("status_reserva"),
    "data_status",
    F.when(F.col("is_canceled") == 1,
           F.datediff("data_chegada", "data_status")).alias("dias_cancel_chegada"),
)

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.reservas")
rejeitados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.rejeitados")

print(f"Silver: {spark.table(f'{CATALOG}.silver.reservas').count():,} reservas")
display(spark.table(f"{CATALOG}.silver.rejeitados").groupBy("motivo_rejeicao").count())

# COMMAND ----------

# Domínios observados (mín./máx.) para o catálogo de dados
display(spark.table(f"{CATALOG}.silver.reservas")
        .select("lead_time", "adr", "noites_total", "hospedes", "receita_potencial", "dias_cancel_chegada")
        .summary("count", "min", "max", "mean"))
