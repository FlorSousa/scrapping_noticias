from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
import os
import sys
from bs4 import BeautifulSoup
import asyncio

load_dotenv()

BASE_DIR = "/opt/airflow"  # caminho fixo dentro do container

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from helpers.db import get_connection, start_database

logger.add(
    os.path.join(BASE_DIR, "logs", "log_scraping.log"),
    rotation="1 MB",
    encoding="utf-8",
    format="{time} {level} {message}",
)

async def get_site_id(name):
    conn = await get_connection()
    try:
        site_id = await conn.fetchval(
            """
            SELECT id FROM site WHERE name = $1;
            """,
            name,
        )
        return site_id
    except Exception as e:
        logger.error(f"Erro na busca. E: {e}")
        return None

async def get_tag_id(tag_name):
    conn = await get_connection()
    try:
        tag_id = await conn.fetchval(
            """
            SELECT id FROM tag WHERE tag_name = $1;
            """,
            tag_name,
        )

        if tag_id is not None:
            return tag_id

        tag_id = await conn.fetchval(
            """
            INSERT INTO tag (tag_name)
            VALUES($1)
            RETURNING id;
            """,
            tag_name,
        )
        return tag_id
    except Exception as e:
        logger.error(f"Erro na busca. E: {e}")
        return None

async def save_new(title, url, site, tag):
    conn = await get_connection()
    try:
        await conn.execute(
            """
            INSERT INTO news (title, url, site_id, tag_id, processed)
            VALUES ($1, $2, $3, $4, $5);
            """,
            title,
            url,
            site,
            tag,
            False,
        )
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar noticia: {title}. E: {e}")
        return False

def filter_olhar_digital(**kwargs):
    localdir = os.path.join(BASE_DIR, "data", "olhar_digital")
    logger.info("Salvando noticias do Olhar Digital")
    if not os.path.exists(localdir):
        logger.error(f"Diretorio não existe: {localdir}")
        return

    tags_list = os.listdir(localdir)
    id_site = asyncio.run(get_site_id("olhar digital"))
    if id_site is None:
        return

    for tag in tags_list:
        id_tag = asyncio.run(get_tag_id(tag))
        file_name = os.listdir(os.path.join(localdir, tag))[0]
        full_path = os.path.join(localdir, tag, file_name)
        with open(full_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")
            article = soup.select_one("div.p-list a")
            url = article["href"]
            title = article["title"]
            sucesso = asyncio.run(save_new(title=title, url=url, site=id_site, tag=id_tag))
            if sucesso:
                logger.success(f"Noticia da tag {tag} salva")

def filter_globo(**kwargs):
    localdir = os.path.join(BASE_DIR, "data", "globo")
    logger.info("Salvando noticias do G1")
    if not os.path.exists(localdir):
        logger.error(f"Diretorio não existe: {localdir}")
        return

    tags_list = os.listdir(localdir)
    id_site = asyncio.run(get_site_id("g1"))
    if id_site is None:
        return

    for tag in tags_list:
        id_tag = asyncio.run(get_tag_id(tag))
        file_name = os.listdir(os.path.join(localdir, tag))[0]
        full_path = os.path.join(localdir, tag, file_name)
        with open(full_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")
            article = soup.select_one("div.bastian-page div._evg div._evt")
            tag_html = article.select_one("h2 a")
            url = tag_html.get("href")
            title = tag_html.find("p").get_text(strip=True)
            sucesso = asyncio.run(save_new(title=title, url=url, site=id_site, tag=id_tag))
            if not sucesso:
                logger.error(f"Erro ao salvar noticia da tag: {tag}")
                return
            logger.success(f"Noticia da tag {tag} salva")

def filter_tecnoblog(**kwargs):
    pass

def init_database():
    from asyncio import run

    run(start_database(logger=logger))

@dag(start_date=datetime(2021, 1, 1), schedule="@hourly", catchup=False)
def filter_news():
    start = EmptyOperator(task_id="start")
    task_init_db = PythonOperator(task_id="init_db", python_callable=init_database)
    task_filter_olhar_digital = PythonOperator(task_id="filter_olhar_digital", python_callable=filter_olhar_digital)
    task_filter_globo = PythonOperator(task_id="filter_globo", python_callable=filter_globo)
    task_filter_tecnoblog = PythonOperator(task_id="filter_tecnoblog", python_callable=filter_tecnoblog)
    end = EmptyOperator(task_id="end")
    start >> task_init_db >> [task_filter_olhar_digital, task_filter_globo, task_filter_tecnoblog] >> end

dag = filter_news()
