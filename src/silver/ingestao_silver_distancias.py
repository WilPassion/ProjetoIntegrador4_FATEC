# Databricks notebook source
# DBTITLE 1,Ingestão Delta Table - Distâncias
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("distancias.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    DISTANCIA_IDA_KM AS distanciaIdaKm,
    DISTANCIA_VOLTA_KM AS distanciaVoltaKm,
    DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM AS distanciaTotalKm,
    TIPO_RODOVIA AS tipoRodovia
FROM bronze.upsell.viagens
"""

# Executa a query e salva o resultado em um DataFrame
df = spark.sql(query)

# Caminho para salvar a Delta Table (ajustado para silver.upsell.viagens)
delta_table_path = "dbfs:/mnt/delta/tables/silver/upsell/"

# Limpa o diretório se não for uma tabela Delta
dbutils.fs.rm(delta_table_path, recurse=True)

# Salva o DataFrame como uma Delta Table
df.write.format("delta").mode("overwrite").save(delta_table_path)
