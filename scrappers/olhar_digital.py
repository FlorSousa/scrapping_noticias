import requests
from loguru import logger
import os

BASE_DIR = "/opt/airflow"
LOG_PATH = os.path.join(BASE_DIR, "logs", "log_scraping.log")
DATA_DIR = os.path.join(BASE_DIR, "data", "olhar_digital")

logger.add(LOG_PATH, rotation="1 MB", encoding="utf-8", format="{time} {level} {message}")

def make_folder(tag):
    try:
        tag_path = os.path.join(DATA_DIR, tag)
        os.makedirs(tag_path, exist_ok=True)
        return tag_path
    except Exception as e:
        logger.error(f"Erro ao criar pasta: {e}")


def get_html(tag):
    try:
        url = f"https://olhardigital.com.br/tag/{tag}/"
        response = requests.get(url)
        if response.status_code == 200:
            dir_path = make_folder(tag)
            file_name = os.path.join(dir_path, f'pagina_olhar_digital_{tag}.html')
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(response.text)
            logger.success(f"Página baixada com sucesso: {url}")
        else:
            logger.error(f"Erro ao baixar página: {url} - Status {response.status_code}")
    except Exception as e:
        logger.error(f"Erro ao criar pasta para a tag '{tag}': {e}")


def scrapping_olhar_digital(**kwargs):
    logger.info("Iniciando Scraping do site: Olhar Digital")
    tag_list = [
        "tecnologia", "api", "pro", "carros-e-tecnologia", 
        "internet-e-redes-sociais", "ciencia-e-espaco", "seguranca"
    ]
    for tag in tag_list:
        get_html(tag)
