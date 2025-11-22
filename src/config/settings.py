# src/config/settings.py
from pathlib import Path
import yaml

class Settings:
    
    def __init__(self, file_name: str = "settings.yml"):
        """Inicializa a classe Settings.
        :param file_name: Nome do arquivo de configuração YAML."""
        self.current_file = Path(__file__).resolve()
        self.src_dir = self.current_file.parent
        self.root_dir =  self.src_dir.parent
        self.settings_path = self.src_dir / file_name
        
        self.config = self._load()
        
    def _load(self) -> dict:
        """Carrega um arquivo de configuração YAML.
        :param path: Caminho para o arquivo de configuração YAML."""
        with open(self.settings_path, "r") as file:
            return yaml.safe_load(file)
            
    
    def get_config(self) -> dict:
        """Retorna o dicionário de configuração carregado."""
        return self.config