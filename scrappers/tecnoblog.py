import requests
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = "/opt/airflow"
LOG_PATH = os.path.join(BASE_DIR, "logs", "log_scraping.log")
DATA_DIR = os.path.join(BASE_DIR, "data", "tecnoblog", "noticias")

logger.add(LOG_PATH, rotation="1 MB", encoding="utf-8", format="{time} {level} {message}")

def make_folder():
    os.makedirs(DATA_DIR, exist_ok=True)
    return DATA_DIR

def get_html():
    url_base = "https://tecnoblog.net/noticias/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    response = requests.get(url_base, headers=headers)
    make_folder()
    if 200 <= response.status_code < 300:
        file_name = os.path.join(DATA_DIR, 'pagina_tecnoblog.html')
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.success(f"Página baixada com sucesso: {url_base}")
    else:
        logger.error(f"Erro ao baixar página: {url_base} - Status {response.status_code}")

def scrapping_tecnoblog(**kwargs):
    logger.info("Iniciando Scraping do site: Tecnoblog")
    get_html()
