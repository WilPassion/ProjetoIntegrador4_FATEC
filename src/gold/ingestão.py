# Databricks notebook source
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


