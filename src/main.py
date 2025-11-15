# src/main.py
from pyspark.sql.functions import col, year
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, LongType, 
                               ArrayType, DateType, FloatType, TimestampType, BooleanType)
from config.settings import carregar_config

config = carregar_config()
app_name = config['spark']['app_name']

spark = SparkSession.builder.appName(app_name).getOrCreate()

schema_pedidos = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", FloatType(), True),
    StructField("QUANTIDADE", LongType(), True),
    StructField("DATA_CRIACAO", DateType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", FloatType(), True),
])

print("Abrindo o dataframe de pedidos")
path_pedidos = config['paths']['pedidos']
compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']
pedidos = spark.read.option("compression", compression_pedidos).csv(path_pedidos, header=True, schema=schema_pedidos, sep=separator_pedidos)
 
print("Renomear colunas do dataframe de pedidos")       
pedidos_df = (
    pedidos_df.withColumnRenamed("ID_PEDIDO", "id_pedido")
        .withColumnRenamed("PRODUTO", "produto")
        .withColumnRenamed("VALOR_UNITARIO", "valor_unitario")
        .withColumnRenamed("QUANTIDADE", "quantidade")
        .withColumnRenamed("DATA_CRIACAO", "data_criacao")
        .withColumnRenamed("UF", "uf")
        .withColumnRenamed("ID_CLIENTE", "id_cliente")
    )

pedidos_df.show(5, truncate=False)

schema_pagamentos = StructType([
    StructField("id_pedido", StringType(), True),     # UUID como string
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", FloatType(), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", TimestampType(), True),
    StructField(
        "avaliacao_fraude",
        StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", FloatType(), True),
        ]),
        True
    )
])

print("Abrindo o dataframe de pagamentos")
path_pagamentos = config['paths']['pagamentos']
pagamentos_df = spark.read.option("compression", "gzip").json(path_pagamentos, schema=schema_pagamentos)

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