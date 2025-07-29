import requests
from loguru import logger
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env, se necessário
load_dotenv()

# Diretório base no container (Airflow monta tudo em /opt/airflow)
BASE_DIR = "/opt/airflow"
LOG_PATH = os.path.join(BASE_DIR, "logs", "log_scraping.log")
DATA_DIR = os.path.join(BASE_DIR, "data", "globo")

# Configura log
logger.add(LOG_PATH, rotation="1 MB", encoding="utf-8", format="{time} {level} {message}")

def make_folder(tag):
    tag_path = os.path.join(DATA_DIR, tag)
    os.makedirs(tag_path, exist_ok=True)
    return tag_path

def get_html(tag):
    url_base = f"https://g1.globo.com/{tag}"
    response = requests.get(url_base)
    if response.status_code == 200:
        dir_path = make_folder(tag)
        file_path = os.path.join(dir_path, f'pagina_globo_{tag}.html')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.success(f"Página baixada com sucesso: {url_base}")
    else:
        logger.error(f"Erro ao baixar página: {url_base} - Status {response.status_code}")

def scrapping_globo(**kwargs):
    logger.info("Iniciando Scraping do site: Globo")
    query_list = ["tecnologia", "ciencia"]
    for tag in query_list:
        get_html(tag)
