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

def test_filter_status(spark_session):
        
    transformation = Transformation()

    pedidos_df = spark_session.createDataFrame(
        [
            Row(
                id_pedido=1,
                uf="SP",
                status=False,
                avaliacao_fraude=Row(fraude=False),
                data_criacao=datetime(2025, 1, 1),
                valor_unitario=10.0,
                quantidade=2,
            ),
            Row(
                id_pedido=2,
                uf="RJ",
                status=True,
                avaliacao_fraude=Row(fraude=False),
                data_criacao=datetime(2025, 1, 1),
                valor_unitario=20.0,
                quantidade=1,
            ),
        ]
    )

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
            (2, "Boleto"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)

    assert resultado_df.count() == 1, "O número de linhas não corresponde o esperado"
    assert resultado_df.first().id_pedido == 1, "O identificador do resultado do calculo não confere com o esperado."
    
def test_filter_fraude(spark_session):
        
    transformation = Transformation()

    pedidos_df = spark_session.createDataFrame([
        Row(
            id_pedido=1,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 1),
            valor_unitario=10.0,
            quantidade=1,
        ),
        Row(
            id_pedido=2,
            uf="RJ",
            status=False,
            avaliacao_fraude=Row(fraude=True),   # deve ser filtrado
            data_criacao=datetime(2025, 1, 1),
            valor_unitario=20.0,
            quantidade=2,
        ),
    ])

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
            (2, "Cartao"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)
    rows = resultado_df.collect()
    row = rows[0]
    
    assert len(rows) == 1, "O número de linhas deve retornar apenas um registro sem fraude"
    assert row.id_pedido == 1, "O identificador do resultado do calculo não confere com o esperado."
    assert row.uf == "SP", "A primeira linha retornada era esperado São Paulo (SP) como UF"
    assert row.forma_pagamento == "Pix", "A forma de pagamento não corresponde o valor Pix esperado"
    
def test_filter_year_2025(spark_session):
    
    transformation = Transformation()

    pedidos_data = [
        Row(
            id_pedido=1,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 5, 1),
            valor_unitario=10.0,
            quantidade=1,
        ),
        Row(
            id_pedido=2,
            uf="RJ",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2024, 5, 1),
            valor_unitario=20.0,
            quantidade=2,
        ),
    ]

    pedidos_df = spark_session.createDataFrame(pedidos_data)

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
            (2, "Cartao"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)
    rows = resultado_df.collect()

    assert len(rows) == 1, "Apenas pedidos de 2025 deveriam ser retornados"
    assert rows[0].id_pedido == 1, "O identificador do resultado do calculo não confere com o esperado."
    assert rows[0].data_criacao.year == 2025, "O ano de criação da linha deve ser 2025"


def test_valor_total_calculo(spark_session):
    
    transformation = Transformation()

    pedidos_data = [
        Row(
            id_pedido=1,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 1),
            valor_unitario=10.0,
            quantidade=3,   # 10 * 3 = 30
        ),
    ]
    
    pedidos_df = spark_session.createDataFrame(pedidos_data)

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)
    row = resultado_df.first()

    assert row.valor_total == 30.0, "valor_total deveria ser 30.0"
    assert row.id_pedido == 1, "O identificador do resultado do calculo não confere com o esperado."
    assert row.forma_pagamento == "Pix", "A forma de pagamento não corresponde o valor Pix esperado"
    
def test_ordering_uf_forma_pagamento_data(spark_session):
    
    transformation = Transformation()

    pedidos_data = [
        Row(
            id_pedido=1,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 2),
            valor_unitario=10.0,
            quantidade=1,
        ),
        Row(
            id_pedido=2,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 1),
            valor_unitario=20.0,
            quantidade=1,
        ),
        Row(
            id_pedido=3,
            uf="RJ",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 3),
            valor_unitario=30.0,
            quantidade=1,
        ),
    ]
    
    pedidos_df = spark_session.createDataFrame(pedidos_data)

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
            (2, "Boleto"),
            (3, "Cartao"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)

    rows = resultado_df.collect()
    atual = [(r.uf, r.forma_pagamento, r.data_criacao, r.id_pedido) for r in rows]

    esperado = sorted(atual, key=lambda x: (x[0], x[1], x[2]))

    assert atual == esperado, "DataFrame não está ordenado por uf, forma_pagamento e data_criacao"

def test_final_schema(spark_session):
    
    transformation = Transformation()

    pedidos_data = [
        Row(
            id_pedido=1,
            uf="SP",
            status=False,
            avaliacao_fraude=Row(fraude=False),
            data_criacao=datetime(2025, 1, 1),
            valor_unitario=10.0,
            quantidade=1,
        )
    ]
    
    pedidos_df = spark_session.createDataFrame(pedidos_data)

    pagamentos_df = spark_session.createDataFrame(
        [
            (1, "Pix"),
        ],
        ["id_pedido", "forma_pagamento"],
    )

    resultado_df = transformation.calculate(pedidos_df, pagamentos_df)

    assert set(resultado_df.columns) == {
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_criacao",
    }, "Schema final não bate com o esperado"