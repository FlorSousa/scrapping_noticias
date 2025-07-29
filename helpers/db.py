import asyncpg
import os
from dotenv import load_dotenv
#load_dotenv()
#load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))
#load_dotenv(dotenv_path="/opt/airflow/.env")
async def get_connection():
    return await asyncpg.connect(
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        database=os.environ.get("POSTGRES_DB"),
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT")
    )

async def start_database(logger):
    logger.debug(os.environ.get("POSTGRES_HOST"))
    print("host:", os.environ.get("POSTGRES_HOST"))
    try:
        conn = await asyncpg.connect(
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            database=os.environ.get("POSTGRES_DB"),
            host=os.environ.get("POSTGRES_HOST"),
            port=os.environ.get("POSTGRES_PORT")
        )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS site(
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE
            );
        """)
        await conn.execute(""" 
            CREATE TABLE IF NOT EXISTS tag(
                id SERIAL PRIMARY KEY,
                tag_name TEXT UNIQUE
            );
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL, 
                site_id INTEGER NOT NULL REFERENCES site(id) ON DELETE CASCADE,
                tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
                processed BOOLEAN DEFAULT FALSE
            );
        """)

        sites_list = [("g1",), ("olhar digital",), ("tecnoblog",)]
        await conn.executemany("""
            INSERT INTO site (name) VALUES ($1)
            ON CONFLICT (name) DO NOTHING;
        """, sites_list)

        await conn.close()
        logger.success("Tabelas criadas com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
