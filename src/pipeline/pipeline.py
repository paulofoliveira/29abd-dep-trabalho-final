# src/pipeline/pipeline.py
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Classe que orquestra a execução do pipeline de dados.
    """

    def __init__(self, spark, data_handler, transformation, config):

        self.spark = spark
        self.data_handler = data_handler
        self.transformation = transformation
        self.config = config

    def run(self):
        """Executa o pipeline de dados."""
        logger.info("Pipeline iniciado...")
        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = self.config["paths"]["pedidos"]
        compression_pedidos = self.config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = self.config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = self.config["file_options"]["pedidos_csv"]["sep"]

        pedidos_df = self.data_handler.load_pedidos(
            path_pedidos, compression_pedidos, header_pedidos, separator_pedidos
        )

        pedidos_df.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pagamentos")
        path_pagamentos = self.config["paths"]["pagamentos"]
        pagamentos_df = self.data_handler.load_pagamentos(path_pagamentos)

        pagamentos_df.show(5, truncate=False)

        logger.info(
            "Monta resultado filtrando e selecionando colunas a partir dos dataframes de pedidos e pagamentos"
        )
        resultado_df = self.transformation.calculate(pedidos_df, pagamentos_df)

        resultado_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        path_output = self.config["paths"]["output"]
        resultado_df.write.mode("overwrite").parquet(path_output)
