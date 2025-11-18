# src/main.py
from pyspark.sql.functions import col, year
from pyspark.sql import SparkSession
from config.settings import carregar_config
from io_utils.data_handler import DataHandler

config = carregar_config()
app_name = config['spark']['app_name']

spark = SparkSession.builder.appName(app_name).getOrCreate()
data_handler = DataHandler(spark)

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

print("Fazendo a junção dos dataframes de pedidos com pagamentos")
pedidos_pagamento_df = pedidos_df.join(pagamentos_df, "id_pedido", "left")

pedidos_pagamento_df.show(5, truncate=False)

print("Monta resulatdo filtrando e selecionando colunas a partir do DF de junção")
filtrado_df = pedidos_pagamento_df.filter((col("status") == False) & (col("avaliacao_fraude.fraude") == False) & (year(col("data_criacao")) == 2025)) \
            .select("id_pedido", 
                    "uf", 
                    "forma_pagamento", 
                    (col("valor_unitario") * col("quantidade")).alias("valor_total"), 
                    "data_criacao").orderBy(
                                    col("uf").asc(),
                                    col("forma_pagamento").asc(),
                                    col("data_criacao").asc()
                                )

filtrado_df.show(20, truncate=False)

print("Escrevendo o resultado em parquet")
path_output = config['paths']['output']
filtrado_df.write.mode("overwrite").parquet(path_output)

spark.stop()