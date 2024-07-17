from state import JsonFileStorage, State, backoff
from postgresextractor import PostgresExtractor
from config import  PostgresConfig, ElasticsearchConfig
from elasticsearch import Elasticsearch
from datetime import datetime
from query import get_movies_query
import logging
import os
from dotenv import load_dotenv
import time
from typing import Optional, List, Dict, Any

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



@backoff()
def fetch_data_with_backoff(extractor: PostgresExtractor, query: str) -> List[Dict[str, Any]]:
    """"
    Извлекает данные из базы данных с использованием заданного SQL-запроса и обрабатывает возможные ошибки.
    
    Args:
    extractor (PostgresExtractor): Экстрактор для извлечения данных из базы данных.
    query (str): SQL-запрос для выполнения.

    Returns:
    List[Dict[str, Any]]: Список словарей, представляющих строки результатов запроса.
    """
 
    return extractor.fetch_data(query)
    

@backoff()
def index_data_with_backoff(es: Elasticsearch, index: str, id: str, document: Dict[str, Any]) -> None:
    """
    Индексирует данные в Elasticsearch, обрабатывая возможные ошибки.

    Args:
    es (Elasticsearch): Клиент Elasticsearch для выполнения операций индексации.
    index (str): Имя индекса, в который будут добавлены данные.
    id (str): Уникальный идентификатор записи для индексации.
    document (Dict[str, Any]): Документ, который нужно индексировать.
    """
    return es.index(index=index, id=id, document=document)

def load_data():
    """
    Основная функция для загрузки данных из PostgreSQL в Elasticsearch.
    """
  
    pg_config = PostgresConfig()
    es_config = ElasticsearchConfig()
    es = Elasticsearch([{"host": es_config.host, "port": es_config.port, "scheme": "http"}])
    state_storage = JsonFileStorage('state.json')
    state = State(state_storage)

    last_processed = state.get_state('last_updated') or '1970-01-01'
    extractor = PostgresExtractor(pg_config.dict(), state)

    data = []  # Инициализация data перед блоком try
    
    try:
        data = fetch_data_with_backoff(extractor, get_movies_query(last_processed))
        logging.info(f"Предоставляет {data}")
        if not data:
            logging.info("Нет данных для обработки.")
        else:
            logging.info(f"Обработка {len(data)} записей.")
            for record in data:
                document = {
                    'id': record['id'],
                    'rating': record.get('rating'),
                    'title': record.get('title'),
                    'description': record.get('description'),
                    'genres': record.get('genres'),
                    'directors_name': record.get('directors_name'),
                    'actors_name': record.get('actors_name'),
                    'writers_name': record.get('writers_name')
                }
                index_data_with_backoff(es, "movies", document['id'], document)
                state.set_state('last_updated', datetime.now().isoformat())

    except Exception as e:
        logging.error(f"Ошибка в процессе загрузки данных: {e}")

    finally:
        extractor.close()


    
        
if __name__ == "__main__":
    while True:
        load_data()
        time.sleep(20)
# Закрытие соединения
