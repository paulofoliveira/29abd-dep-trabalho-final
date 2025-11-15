# src/main.py (Versão 2: Com Schema Explícito)
from pyspark.sql.functions import col, year
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, LongType, 
                               ArrayType, DateType, FloatType, TimestampType, BooleanType)

spark = SparkSession.builder.appName("Trabalho Final - Data Engeering Programming").getOrCreate()

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
pedidos_df = spark.read.option("compression", "gzip").csv("data/input/pedidos", header=True, schema=schema_pedidos, sep=";")
        
 
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
pagamentos_df = spark.read.option("compression", "gzip").json("data/input/pagamentos", schema=schema_pagamentos)

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
filtrado_df.write.mode("overwrite").parquet("data/output/relatorio")

spark.stop()