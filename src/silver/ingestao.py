# Databricks notebook source
# DBTITLE 1,Ingestão  DeltaTable - Viagens
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

tablename = "viagens"
camada = "silver"

query = import_query(tablename + ".sql")
print(query)

df = spark.sql(query)
df.write.format("delta").mode("overwrite").saveAsTable(f"{camada}.upsell.{tablename}")
