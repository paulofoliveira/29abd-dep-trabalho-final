# src/main.py
from config.settings import carregar_config
from io_utils.data_handler import DataHandler
from session.spark_session import SparkSessionManager
from processing.transformations import Transformation

config = carregar_config()
app_name = config['spark']['app_name']
spark = SparkSessionManager.get_spark_session(app_name)
data_handler = DataHandler(spark)
transformation = Transformation()

print("Abrindo o dataframe de pedidos")
path_pedidos = config['paths']['pedidos']
compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']

pedidos_df = data_handler.load_pedidos(path_pedidos, compression_pedidos, header_pedidos, separator_pedidos)
 
pedidos_df.show(5, truncate=False)

print("Abrindo o dataframe de pagamentos")
path_pagamentos = config['paths']['pagamentos']
pagamentos_df = data_handler.load_pagamentos(path_pagamentos)

pagamentos_df.show(5, truncate=False)

print("Monta resultado filtrando e selecionando colunas a partir dos dataframes de pedidos e pagamentos")
filtrado_df = transformation.calculate(pedidos_df, pagamentos_df)

filtrado_df.show(20, truncate=False)

print("Escrevendo o resultado em parquet")
path_output = config['paths']['output']
filtrado_df.write.mode("overwrite").parquet(path_output)

spark.stop()