# Databricks notebook source
# DBTITLE 1,Ingestão Delta Table  - Viagens
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("viagens.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    DES AS destino,
    EMISSAO AS dataEmissao,
    VEICULO AS veiculo,
    MOTORISTA AS motorista,
    TOT_RECEB AS totalRecebido,
    CLIENTE_ORIGEM AS clienteOrigem,
    ORIGEM_CIDADE AS origemCidade,
    ORIGEM_ESTADO AS origemEstado,
    CLIENTE_COLETA AS clienteColeta,
    COLETA_CIDADE AS coletaCidade,
    COLETA_ESTADO AS coletaEstado,
    DISTANCIA_IDA_KM AS distanciaIdaKm,
    DISTANCIA_VOLTA_KM AS distanciaVoltaKm,
    TEMPO_ESTIM_IDA_MIN AS tempoEstimadoIda,
    TEMPO_ESTIM_VOLTA_MIN AS tempoEstimadoVolta,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    CAPACIDADE_CARGA_TONELADAS AS capacidadeCargaToneladas,
    UTILIZACAO_VEICULO AS utilizacaoVeiculo,
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

# COMMAND ----------

# DBTITLE 1,Ingestão Delta Table - Consumo
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("consumo.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    EMISSAO AS dataEmissao,
    VEICULO AS veiculo,
    ORIGEM_CIDADE AS origemCidade,
    ORIGEM_ESTADO AS origemEstado,
    COLETA_CIDADE AS coletaCidade,
    COLETA_ESTADO AS coletaEstado,
    DISTANCIA_IDA_KM AS distanciaIdaKm,
    DISTANCIA_VOLTA_KM AS distanciaVoltaKm,
    TEMPO_ESTIM_IDA_MIN AS tempoEstimadoIda,
    TEMPO_ESTIM_VOLTA_MIN AS tempoEstimadoVolta,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    CAPACIDADE_CARGA_TONELADAS AS capacidadeCargaToneladas,
    UTILIZACAO_VEICULO AS utilizacaoVeiculo,
    TIPO_RODOVIA AS tipoRodovia,
    TOT_RECEB AS totalRecebido,
    CLIENTE_ORIGEM AS clienteOrigem,
    CLIENTE_COLETA AS clienteColeta
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

# COMMAND ----------

# DBTITLE 1,Ingestão Delta Table - Clientes
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("clientes.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    EMISSAO AS dataEmissao,        
    CLIENTE_ORIGEM AS clienteOrigem,
    ORIGEM_CIDADE AS origemCidade,
    ORIGEM_ESTADO AS origemEstado,
    CLIENTE_COLETA AS clienteColeta,
    COLETA_CIDADE AS coletaCidade,
    COLETA_ESTADO AS coletaEstado
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

# COMMAND ----------

# DBTITLE 1,Ingestão Delta Table - Custos
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("custos.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM AS distanciaTotalKm,
    (CUSTO_COMBUSTIVEL_IDA + CUSTO_COMBUSTIVEL_VOLTA) / 
    (DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM) AS custoPorKm
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

# COMMAND ----------

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

# COMMAND ----------

# DBTITLE 1,Ingestão Delta Table - Receita
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

query = import_query("receita.sql")
print(query)

query = """
SELECT
    ID_viagem AS idViagem,
    TOT_RECEB AS totalRecebido,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    TOT_RECEB - (CUSTO_COMBUSTIVEL_IDA + CUSTO_COMBUSTIVEL_VOLTA) AS lucroLiquido,
    CLIENTE_ORIGEM AS clienteOrigem,
    CLIENTE_COLETA AS clienteColeta
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
