# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    DateType,
    FloatType,
    TimestampType,
    BooleanType,
)
from pyspark.errors import AnalysisException
import logging

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        """Inicializa o DataHandler com uma sessão Spark.
        :param spark: Instância da SparkSession.
        """

        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        """Define o schema para o dataframe de pedidos."""
        return StructType(
            [
                StructField("ID_PEDIDO", StringType(), True),
                StructField("PRODUTO", StringType(), True),
                StructField("VALOR_UNITARIO", FloatType(), True),
                StructField("QUANTIDADE", LongType(), True),
                StructField("DATA_CRIACAO", DateType(), True),
                StructField("UF", StringType(), True),
                StructField("ID_CLIENTE", FloatType(), True),
            ]
        )

    def _get_schema_pagamentos(self) -> StructType:
        """Define o schema para o dataframe de pagamentos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),  # UUID como string
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", FloatType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def load_pedidos(
        self, path: str, compression: str, header: bool, separator: str
    ) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de arquivos CSV comprimidos.
        :param path: Caminho para os arquivos de pedidos."""
        schema = self._get_schema_pedidos()

        try:
            df = self.spark.read.option("compression", compression).csv(
                path, header=header, schema=schema, sep=separator
            )
            return (
                df.withColumnRenamed("ID_PEDIDO", "id_pedido")
                .withColumnRenamed("PRODUTO", "produto")
                .withColumnRenamed("VALOR_UNITARIO", "valor_unitario")
                .withColumnRenamed("QUANTIDADE", "quantidade")
                .withColumnRenamed("DATA_CRIACAO", "data_criacao")
                .withColumnRenamed("UF", "uf")
                .withColumnRenamed("ID_CLIENTE", "id_cliente")
            )
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pedidos: {e}")
            return

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de arquivos JSON comprimidos.
        :param path: Caminho para os arquivos de pagamentos.
        """
        schema = self._get_schema_pagamentos()
        try:
            return self.spark.read.option("compression", "gzip").json(
                path, schema=schema
            )
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pagamentos: {e}")
            return
