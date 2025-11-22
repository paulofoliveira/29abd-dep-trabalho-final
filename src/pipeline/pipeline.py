# src/pipeline/pipeline.py
from pyspark.sql import SparkSession
from config.settings import Settings
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Classe que orquestra a execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession, data_handler: DataHandler, transformation : Transformation, settings : Settings):
        """Inicializa o pipeline com os componentes necessários.
        :param spark: Instância da SparkSession.
        :param data_handler: Instância do DataHandler para manipulação de dados.
        :param transformation: Instância da Transformation para transformações de dados.
        :param settings: Instância da Settings para configurações do pipeline."""
        
        self.spark = spark
        self.data_handler = data_handler
        self.transformation = transformation
        self.settings = settings

    def run(self):
        """Executa o pipeline de dados."""
        logger.info("Pipeline iniciado...")
        
        config = self.settings.get_config()
         
        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

        pedidos_df = self.data_handler.load_pedidos(
            path_pedidos, compression_pedidos, header_pedidos, separator_pedidos
        )

        pedidos_df.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]
        pagamentos_df = self.data_handler.load_pagamentos(path_pagamentos)

        pagamentos_df.show(5, truncate=False)

        logger.info(
            "Monta resultado filtrando e selecionando colunas a partir dos dataframes de pedidos e pagamentos"
        )

        resultado_df = self.transformation.calculate(pedidos_df, pagamentos_df)

        resultado_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        path_output = config["paths"]["output"]
        resultado_df.write.mode("overwrite").parquet(path_output)