# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Análise: respostas às perguntas de negócio (Q1–Q5)
# MAGIC Cada célula gera os números de uma tabela/parágrafo do paper. **Tire print de cada resultado** para a pasta `/evidencias`.

# COMMAND ----------

dbutils.widgets.text("catalog", "mvp_hotel")
CATALOG = dbutils.widgets.get("catalog")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW r AS
SELECT f.*, h.tipo_hotel, t.data AS data_chegada, t.mes,
       c.segmento, c.tipo_deposito, p.recorrente, p.faixa_cancel_prev
FROM {CATALOG}.gold.fato_reserva f
JOIN {CATALOG}.gold.dim_hotel          h ON f.sk_hotel = h.sk_hotel
JOIN {CATALOG}.gold.dim_tempo          t ON f.sk_data_chegada = t.sk_data
JOIN {CATALOG}.gold.dim_canal          c ON f.sk_canal = c.sk_canal
JOIN {CATALOG}.gold.dim_perfil_cliente p ON f.sk_perfil_cliente = p.sk_perfil
""")
display(spark.sql("SELECT COUNT(*) AS reservas_na_fato FROM r"))

# COMMAND ----------

# MAGIC %md ## Q1 – Taxa de cancelamento por hotel e por mês de chegada

# COMMAND ----------

display(spark.sql("""
SELECT tipo_hotel, COUNT(*) AS reservas,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct
FROM r GROUP BY 1"""))

# COMMAND ----------

# Use a opção de gráfico (+ > Visualization > Line) do display para gerar a figura mensal
display(spark.sql("""
SELECT tipo_hotel, mes, COUNT(*) AS reservas,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct
FROM r GROUP BY 1, 2 ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md ## Q2 – Antecedência (Tabela 4) + teste qui-quadrado

# COMMAND ----------

q2 = spark.sql("""
SELECT CASE WHEN lead_time <= 7   THEN '1) 0-7 dias'
            WHEN lead_time <= 30  THEN '2) 8-30 dias'
            WHEN lead_time <= 90  THEN '3) 31-90 dias'
            WHEN lead_time <= 180 THEN '4) 91-180 dias'
            ELSE                       '5) >180 dias' END AS faixa,
       COUNT(*) AS reservas,
       SUM(CAST(flag_cancelada AS INT)) AS canceladas,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct,
       ROUND(AVG(adr), 1) AS diaria_media
FROM r GROUP BY 1 ORDER BY 1""")
display(q2)

from scipy.stats import chi2_contingency
pdf = q2.toPandas()
tabela = [pdf["canceladas"].tolist(), (pdf["reservas"] - pdf["canceladas"]).tolist()]
chi2, p, gl, _ = chi2_contingency(tabela)
print(f"Qui-quadrado = {chi2:,.1f} | graus de liberdade = {gl} | p-valor = {p:.3g}")

# COMMAND ----------

# MAGIC %md ## Q3 – Segmento x tipo de depósito (Tabela 5)

# COMMAND ----------

display(spark.sql("""
SELECT segmento, tipo_deposito, COUNT(*) AS reservas,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct,
       ROUND(AVG(lead_time)) AS antecedencia_media
FROM r GROUP BY 1, 2
HAVING COUNT(*) >= 200
ORDER BY taxa_pct DESC LIMIT 10"""))

# COMMAND ----------

# MAGIC %md ## Q4 – Receita potencial cancelada

# COMMAND ----------

display(spark.sql("""
SELECT COALESCE(tipo_hotel, 'TOTAL') AS hotel,
       ROUND(SUM(receita_potencial) / 1e6, 2) AS receita_cancelada_mi_eur,
       ROUND(100 * SUM(CASE WHEN dias_cancel_chegada < 7 THEN receita_potencial ELSE 0 END)
                 / SUM(receita_potencial), 1) AS pct_cancelada_ultima_semana
FROM r WHERE flag_cancelada
GROUP BY ROLLUP(tipo_hotel)"""))

# COMMAND ----------

display(spark.sql("""
SELECT date_trunc('month', data_chegada) AS mes, tipo_hotel,
       ROUND(SUM(receita_potencial)) AS receita_cancelada_eur
FROM r WHERE flag_cancelada GROUP BY 1, 2 ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md ## Q5 – Histórico do cliente

# COMMAND ----------

display(spark.sql("""
SELECT recorrente, COUNT(*) AS reservas,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_da_base,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct
FROM r GROUP BY 1"""))

display(spark.sql("""
SELECT faixa_cancel_prev, COUNT(*) AS reservas,
       ROUND(100 * AVG(CAST(flag_cancelada AS INT)), 1) AS taxa_pct
FROM r GROUP BY 1 ORDER BY 1"""))
