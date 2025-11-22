# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year

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
    
    def calculate(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """
        Realiza cálculos específicos nos dataframes.

        :param pedidos_df: DataFrame contendo os dados de pedidos.
        :param pagamentos_df: DataFrame contendo os dados de pagamentos.
        :return: DataFrame resultante dos cálculos.
        """
        pedidos_pagamento_df = self.join_pedidos_pagamentos(pedidos_df, pagamentos_df)

        resultado_df = pedidos_pagamento_df.filter((col("status") == False) & (col("avaliacao_fraude.fraude") == False) & (year(col("data_criacao")) == 2025)) \
            .select("id_pedido", 
                    "uf", 
                    "forma_pagamento", 
                    (col("valor_unitario") * col("quantidade")).alias("valor_total"), 
                    "data_criacao").orderBy(
                                    col("uf").asc(),
                                    col("forma_pagamento").asc(),
                                    col("data_criacao").asc()
                                )
        
        return resultado_df



