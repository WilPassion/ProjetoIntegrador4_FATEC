# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestão Viagens
# MAGIC
# MAGIC Dataset: dados empresa  
# MAGIC
# MAGIC catalog = "bronze"  
# MAGIC schema = "upsell"  
# MAGIC tablename = "viagens"  

# COMMAND ----------

# DBTITLE 1,Leitura Aquivos - Bucket Raw
dbutils.fs.ls("s3://pb-translog-raw/upsell/full_load/viagens/")

# COMMAND ----------

# DBTITLE 1,Load dos Arquivos
# Caminho do S3 onde estão os arquivos Parquet
s3_path = "s3://pb-translog-raw/upsell/full_load/viagens/"

# Carregar todos os arquivos Parquet do caminho
df_full = spark.read.format("parquet").load("s3://pb-translog-raw/upsell/full_load/viagens/")

# COMMAND ----------

# DBTITLE 1,Importação Sessão Spark
# Importando bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, lit, regexp_replace, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Mesclar Arquivos Parquet e Salvar Delta Table") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Corrigindo Colunas
# Renomear colunas para evitar erros causados por caracteres especiais
for col_name in df_full.columns:
    new_col_name = col_name.replace(" ", "_").replace(",", "_").replace(";", "_") \
                           .replace("{", "_").replace("}", "_").replace("(", "_") \
                           .replace(")", "_").replace("\n", "_").replace("\t", "_") \
                           .replace("=", "_")
    df_full = df_full.withColumnRenamed(col_name, new_col_name)

# Corrigir a coluna TOT_RECEB
if "TOT_RECEB" in df_full.columns:
    df_full = df_full.withColumn("TOT_RECEB", 
                                 regexp_replace(regexp_replace(col("TOT_RECEB"), "\\.", ""), ",", ".")  # Remove o ponto e substitui a vírgula
                                 .cast(DoubleType()))  # Converte para double

# Adicionar novas colunas e preencher valores baseados na coluna DES
df_full = df_full \
    .withColumn("CLIENTE_ORIGEM", when(col("DES") == "HOR", lit("Transmac Logistica Integrada"))
                                  .when(col("DES") == "GRU", lit("Transmac Logistica Integrada"))
                                  .when(col("DES") == "CPN", lit("Transmac Logistica Integrada"))) \
    .withColumn("ORIGEM_CIDADE", when(col("DES") == "HOR", lit("Guarulhos"))
                                  .when(col("DES") == "GRU", lit("Guarulhos"))
                                  .when(col("DES") == "CPN", lit("Guarulhos"))) \
    .withColumn("ORIGEM_ESTADO", lit("SP")) \
    .withColumn("CLIENTE_COLETA", when(col("DES") == "HOR", lit("Quimlab Quimica"))
                                   .when(col("DES") == "GRU", lit("ITW Chemical Products Ltda"))
                                   .when(col("DES") == "CPN", lit("MAUSER Packaging Solutions"))) \
    .withColumn("COLETA_CIDADE", when(col("DES") == "HOR", lit("Taubate"))
                                   .when(col("DES") == "GRU", lit("Araras"))
                                   .when(col("DES") == "CPN", lit("Suzano"))) \
    .withColumn("COLETA_ESTADO", lit("SP")) \
    .withColumn("DISTANCIA_IDA_KM", when(col("DES") == "HOR", lit(105))
                                     .when(col("DES") == "GRU", lit(186))
                                     .when(col("DES") == "CPN", lit(29.4))) \
    .withColumn("DISTANCIA_VOLTA_KM", when(col("DES") == "HOR", lit(116))
                                       .when(col("DES") == "GRU", lit(190))
                                       .when(col("DES") == "CPN", lit(26.3))) \
    .withColumn("TEMPO_ESTIM_IDA_MIN", when(col("DES") == "HOR", lit(86))
                                        .when(col("DES") == "GRU", lit(169))
                                        .when(col("DES") == "CPN", lit(42))) \
    .withColumn("TEMPO_ESTIM_VOLTA_MIN", when(col("DES") == "HOR", lit(86))
                                          .when(col("DES") == "GRU", lit(156))
                                          .when(col("DES") == "CPN", lit(36))) \
    .withColumn("CUSTO_COMBUSTIVEL_IDA", when(col("DES") == "HOR", lit(367.5))
                                          .when(col("DES") == "GRU", lit(651.0))
                                          .when(col("DES") == "CPN", lit(102.09))) \
    .withColumn("CUSTO_COMBUSTIVEL_VOLTA", when(col("DES") == "HOR", lit(406.0))
                                            .when(col("DES") == "GRU", lit(665.0))
                                            .when(col("DES") == "CPN", lit(92.05))) \
    .withColumn("CAPACIDADE_CARGA_TONELADAS", lit(28)) \
    .withColumn("UTILIZACAO_VEICULO", lit(1.0)) \
    .withColumn("TIPO_RODOVIA", when(col("DES") == "HOR", lit("FEDERAL"))
                                 .when(col("DES") == "GRU", lit("ESTADUAL"))
                                 .when(col("DES") == "CPN", lit("ESTADUAL")))

# Adicionar coluna de ID sequencial e renomeá-la para ID_viagem
windowSpec = Window.orderBy("EMISSAO")  # Ajuste para ordenar por qualquer coluna desejada
df_full = df_full.withColumn("ID_viagem", row_number().over(windowSpec))

# Reordenar as colunas, colocando ID_viagem como a primeira
columns = ["ID_viagem"] + [col for col in df_full.columns if col != "ID_viagem"]
df_full = df_full.select(*columns)

# COMMAND ----------

# DBTITLE 1,Escrevendo Arquivo/Tabela Delta
# Salvar o DataFrame como uma Delta Table na camada bronze
tablename = "viagens"
camada = "bronze"

(df_full.coalesce(1)
 .write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .saveAsTable(f"{camada}.upsell.{tablename}"))

# Mensagem de sucesso/confirmação
print(f"Detal Table salvo com sucesso na camada {camada}.upsell.{tablename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão Preços Combustíveis
# MAGIC
# MAGIC Dataset: [gov.br](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)
# MAGIC
# MAGIC catalog = "bronze"  
# MAGIC schema = "upsell"  
# MAGIC tablename = "precos-combustiveis"  

# COMMAND ----------

# DBTITLE 1,Leitura Aquivos - Raw Bucket
dbutils.fs.ls("s3://pb-translog-raw/upsell/full_load/precos_combustiveis/")

# COMMAND ----------

# DBTITLE 1,Load dos Arquivos
# Caminho do S3 onde estão os arquivos Parquet
s3_path = "s3://pb-translog-raw/upsell/full_load/precos_combustiveis/"

# Carregar todos os arquivos Parquet do caminho
df_full = spark.read.format("parquet").load(s3_path)

# COMMAND ----------

# DBTITLE 1,Escrevendo Delta Table
# Salvar o DataFrame como uma Delta Table na camada bronze
tablename = "precos_combustiveis"
camada = "bronze"

(df_full.coalesce(1)
 .write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .saveAsTable(f"{camada}.upsell.{tablename}"))

# Mensagem de sucesso/confirmação
print(f"Delta Table salvo com sucesso na camada {camada}.upsell.{tablename}")

# COMMAND ----------

# MAGIC %md
# MAGIC
