# Databricks notebook source
# Importar SparkSession
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

# COMMAND ----------

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Consulta Preços Combustíveis") \
    .getOrCreate()

# Consulta no formato PySpark
df_info = spark.sql("""
    SELECT 
        valorVenda AS valorVenda,
        dataColeta AS dataColeta,
        produto AS produto
    FROM bronze.upsell.`precos_combustiveis`
""")

# Convert to Pandas DataFrame
df = df_info.toPandas()

display(df)

# COMMAND ----------

#Filtrando apenas as variáveis necessárias
df.columns=['valorVenda', 'dataColeta', 'produto']

display(df.columns)

# COMMAND ----------

#Alterando tipo das variáveis
df['dataColeta']=pd.to_datetime(df['dataColeta'], dayfirst=True)

df['valorVenda']=df['valorVenda'].astype(str).str.replace(',', '.').astype(float)

# COMMAND ----------

#Tamanho da figura
plt.figure(figsize=(18, 9))

#Gráfico
sns.lineplot(data=df,
             x="dataColeta",
             y="valorVenda",
             hue='produto',
             style='produto',
             markers=False,
             dashes=False,
             palette='deep',
             linewidth=2)

#Plot
plt.title("Variação do preço de combustíveis", fontsize=15, y=1.03)

# COMMAND ----------

#Tamanho da figura
plt.figure(figsize=(12, 5))

#Gráfico
sns.countplot(x=df['produto'],
              palette='deep')

#Plot
plt.title("Distribuição dos combustíveis mais vendidos da amostra", fontsize=15, y=1.03)

# COMMAND ----------

#Tamanho da figura
plt.figure(figsize=(10, 4))

#Gráfico
sns.boxplot(x=df['valorVenda'],
            y=df['produto'] ,
            data=df,
            orient='h',
            palette='deep')

#Plot
plt.xlabel('Valores')
plt.ylabel('Categorias')
plt.show()

# COMMAND ----------

#DIESEL
df_diesel = df[df['produto'] == 'DIESEL'][['dataColeta', 'valorVenda']]

  #Agrupamento por média de valores
df_diesel = df_diesel.groupby('dataColeta', as_index=False)['valorVenda'].mean()


#DIESEL S10
df_diesel_s10 = df[df['produto'] == 'DIESEL S10'][['dataColeta', 'valorVenda']]

  #Agrupamento por média de valores
df_diesel_s10 = df_diesel_s10.groupby('dataColeta', as_index=False)['valorVenda'].mean()

# COMMAND ----------

#Ajuste processamento
df_diesel['dataColeta'] = pd.to_datetime(df_diesel['dataColeta'])
df_diesel.set_index('dataColeta', inplace=True)

# COMMAND ----------

#Treinamento
def treino_arima_e_forecast(data, steps=30):
    model = ARIMA(data, order=(5, 1, 0))
    model_fit=model.fit()
    forecast=model_fit.forecast(steps=steps)
    return forecast

#MAPE
def calculando_mape(atual, forecast):
    return (np.abs(atual-forecast)/atual).mean() * 100

df_diesel=df_diesel.resample('D').mean()

#Ajuste 30 dias
forecast=treino_arima_e_forecast(df_diesel['valorVenda'], steps=30)

forecast_dates=pd.date_range(df_diesel.index[-1]+pd.Timedelta(days=1), periods=30, freq='D')
forecast_series=pd.Series(forecast, index=forecast_dates)

#Plot
plt.figure(figsize=(10, 6))
plt.plot(df_diesel.index, df_diesel['valorVenda'], label='Valor Histórico', color='blue')
plt.plot(forecast_series.index, forecast_series, label='Previsão', color='red')
plt.title('Previsão DIESEL - ARIMA')
plt.xlabel('Data')
plt.ylabel('Valor de Venda')
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------



# COMMAND ----------

import boto3
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt

# Função de treinamento e previsão com ARIMA
def treino_arima_e_forecast(data, steps=30):
    model = ARIMA(data, order=(5, 1, 0))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=steps)
    return forecast

# Função para calcular o MAPE
def calculando_mape(atual, forecast):
    return (np.abs(atual - forecast) / atual).mean() * 100

# Resample diário dos dados
df_diesel = df_diesel.resample('D').mean()

# Ajuste de 30 dias
forecast = treino_arima_e_forecast(df_diesel['valorVenda'], steps=30)

# Criação da série de datas para o período de previsão
forecast_dates = pd.date_range(df_diesel.index[-1] + pd.Timedelta(days=1), periods=30, freq='D')
forecast_series = pd.Series(forecast, index=forecast_dates)

# Salvando os resultados como um DataFrame para exportação em Parquet
forecast_df = pd.DataFrame({
    'dataColeta': forecast_series.index,
    'forecast': forecast_series.values
})

# Salvando o DataFrame como Parquet no S3
forecast_df.to_parquet
