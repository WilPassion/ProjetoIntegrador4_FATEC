# Databricks notebook source
# DBTITLE 1,Ingestão - daily_report
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

tablename = "daily_report"
camada = "gold"

query = import_query(tablename + ".sql")
print(query)

df = spark.sql(query)
df.write.format("delta").mode("overwrite").saveAsTable(f"{camada}.upsell.{tablename}")

# COMMAND ----------

# DBTITLE 1,Ingestão - monthly_report
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

tablename = "monthly_report"
camada = "gold"

query = import_query(tablename + ".sql")
print(query)

df = spark.sql(query)
df.write.format("delta").mode("overwrite").saveAsTable(f"{camada}.upsell.{tablename}")
