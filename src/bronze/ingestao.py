# Databricks notebook source
# DBTITLE 1,Leitura Aquivos - Bucket
dbutils.fs.ls("s3://translog-pb-raw/upsell/full_load/viagens/")

# COMMAND ----------

# DBTITLE 1,Importação Sessão Spark
# Importando bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Mesclar Arquivos Parquet e Salvar Delta Table") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .enableHiveSupport() \
    .getOrCreate() 

# COMMAND ----------

# DBTITLE 1,Load dos Arquivos
# Caminho do S3 onde estão os arquivos Parquet
s3_path = "s3://translog-pb-raw/upsell/full_load/viagens/"

# Carregar todos os arquivos Parquet do caminho
df_full = spark.read.format("parquet").load("s3://translog-pb-raw/upsell/full_load/viagens/")

# COMMAND ----------

# DBTITLE 1,Corrigindo Colunas
# Renomear colunas para evitar erros causados por caracteres especiais
for col_name in df_full.columns:
    new_col_name = col_name.replace(" ", "_").replace(",", "_").replace(";", "_") \
                           .replace("{", "_").replace("}", "_").replace("(", "_") \
                           .replace(")", "_").replace("\n", "_").replace("\t", "_") \
                           .replace("=", "_")
    df_full = df_full.withColumnRenamed(col_name, new_col_name)

# Corrigindo Schema/Colunas - 'DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB'
# Dividir a coluna em partes separadas com base na vírgula
if "DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB" in df_full.columns:
    df_full = df_full.withColumn("DES", split(col("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB"), ",")[0]) \
                     .withColumn("EMISSAO", split(col("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB"), ",")[1]) \
                     .withColumn("VEICULO", split(col("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB"), ",")[2]) \
                     .withColumn("MOTORISTA", split(col("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB"), ",")[3]) \
                     .withColumn("TOT_RECEB", split(col("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB"), ",")[4]) \
                     .drop("DES_EMISSAO_VEICULO_MOTORISTA_TOT_RECEB")

# COMMAND ----------

# MAGIC %md
# MAGIC catalog = "bronze"  
# MAGIC schema = "upsell"  
# MAGIC tablename = "viagens"  

# COMMAND ----------

# DBTITLE 1,Escrevendo Arquivo/Tabela Delta
# Salvar o DataFrame como uma Delta Table na camada bronze
(df_full.coalesce(1)
 .write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .saveAsTable("bronze.upsell.viagens"))

# Mensagem de sucesso/confirmação
print("Delta Table salva como bronze.upsell.viagens com sucesso!")
