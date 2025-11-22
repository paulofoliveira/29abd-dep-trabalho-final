# src/config/settings.py
import yaml


def carregar_config(path: str = "config/settings.yml") -> dict:
    """Carrega um arquivo de configuração YAML.
    :param path: Caminho para o arquivo de configuração YAML."""
    with open(path, "r") as file:
        return yaml.safe_load(file)
