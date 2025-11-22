# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from src.processing.transformations import Transformation
from pyspark.sql import Row
from datetime import datetime

@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_join_pedidos_pagamentos(spark_session):
    
    transformation = Transformation()
    
    pedidos_df = spark_session.createDataFrame([
        (1, "SP"), 
        (2, "RJ")
    ], ["id_pedido", "uf"])

    pagamentos_df = spark_session.createDataFrame([
        (1, "Pix")
    ], ["id_pedido", "forma_pagamento"])

    pedidos_pagamentos_df = transformation.join_pedidos_pagamentos(pedidos_df, pagamentos_df)
    
    row_p1 = pedidos_pagamentos_df.filter("id_pedido = 1").collect()[0]
    row_p2 = pedidos_pagamentos_df.filter("id_pedido = 2").collect()[0]

    assert pedidos_pagamentos_df.count() == 2, "O número de linhas não corresponde o esperado"
    assert row_p1.forma_pagamento == "Pix", "A forma de pagamento não corresponde o valor Pix esperado"
    assert row_p2.forma_pagamento is None, "A forma de pagamento não está nula representando que há um pedido sem pagamento"