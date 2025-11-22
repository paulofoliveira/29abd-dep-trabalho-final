# src/processing/transformations.py
from pyspark.sql import DataFrame

class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """
    def join_pedidos_pagamentos(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """
        Realiza a junção dos dataframes de pedidos e pagamentos.

        :param pedidos_df: DataFrame contendo os dados de pedidos.
        :param pagamentos_df: DataFrame contendo os dados de pagamentos.
        :return: DataFrame resultante da junção.
        """
        return pedidos_df.join(pagamentos_df, "id_pedido", "left")

