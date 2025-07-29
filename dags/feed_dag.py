from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Define o caminho base dentro do container
BASE_DIR = "/opt/airflow"

# Adiciona o diretório base ao sys.path para importar seus módulos
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from scrappers.olhar_digital import scrapping_olhar_digital
from scrappers.globo import scrapping_globo
from scrappers.tecnoblog import scrapping_tecnoblog

@dag(start_date=datetime(2021, 1, 1), schedule="@hourly", catchup=False)
def get_news():
    start = EmptyOperator(task_id="start")
    olhar_digital = PythonOperator(task_id="olhar_digital", python_callable=scrapping_olhar_digital)
    globo = PythonOperator(task_id="globo", python_callable=scrapping_globo)
    tecnoblog = PythonOperator(task_id="tecnoblog", python_callable=scrapping_tecnoblog)
    end = EmptyOperator(task_id="fim_scraping")
    start >> [olhar_digital, globo, tecnoblog] >> end

dag = get_news()
