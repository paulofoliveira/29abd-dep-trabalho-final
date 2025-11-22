# src/main.py
from config.settings import carregar_config
from io_utils.data_handler import DataHandler
from session.spark_session import SparkSessionManager
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline
import logging

def configurar_logging(log_name: str):
  """Configura o logging para todo o projeto."""
  logging.basicConfig(

      level=logging.INFO,
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',

      handlers=[
          logging.FileHandler(log_name), # Log para arquivo
          logging.StreamHandler()                         # Log para o console (terminal)
      ]
  )

  logging.info("Logging configurado.")


def main():
    """
    Função principal que atua como a "Raiz de Composição".
    Configura e executa o pipeline.
    """
    
    config = carregar_config()
    app_name = config['spark']['app_name']
    spark = SparkSessionManager.get_spark_session(app_name)
    data_handler = DataHandler(spark)
    transformation = Transformation()

    pipeline = Pipeline(spark, data_handler, transformation, config)
    pipeline.run()

    spark.stop()

if __name__ == "__main__":
    configurar_logging("pipeline.log"))
    main()