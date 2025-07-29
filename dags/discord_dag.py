from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import sys
import asyncio
import requests
import time
from loguru import logger
from dotenv import load_dotenv

#load_dotenv()

BASE_DIR = "/opt/airflow"

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

logger.add(
    os.path.join(BASE_DIR, "logs", "log_scraping.log"),
    rotation="1 MB",
    encoding="utf-8",
    format="{time} {level} {message}",
)

from helpers.db import get_connection, start_database

def send_pending_news():
    from asyncio import run

    async def _send():
        conn = await get_connection()
        rows = await conn.fetch("SELECT * FROM news WHERE processed = FALSE;")
        webhook_url = os.environ.get("WEBHOOK_DISCORD")
        logger.debug(webhook_url)

        for row in rows:
            formated_msg = f"# {row['title']}\n{row['url']}"
            data = {"content": formated_msg}
            try:
                response = requests.post(webhook_url, json=data)
                response.raise_for_status()
                logger.success(f"Noticia enviada: {row['id']}")
            except Exception as e:
                logger.error(f"Falha ao enviar noticia. E: {e}")
            time.sleep(2)

        #await conn.execute("UPDATE news SET processed = TRUE WHERE processed = FALSE;")

    run(_send())

@dag(start_date=datetime(2021, 1, 1), schedule="@hourly", catchup=False)
def send_news():
    start = EmptyOperator(task_id="start")
    task_send = PythonOperator(task_id="send_pending_news", python_callable=send_pending_news)
    end = EmptyOperator(task_id="end")
    start >> task_send >> end

dag = send_news()
