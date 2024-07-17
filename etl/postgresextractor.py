import psycopg2
import psycopg2.extras
import json
import logging

class PostgresExtractor:
    def __init__(self, connection_params, state):
        self.connection = psycopg2.connect(**connection_params)
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.state = state

    def fetch_data(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            # Здесь мы логируем исходное исключение и перевыбрасываем его
            logging.error(f"Ошибка выполнения запроса: {e}")
            raise


    def close(self):
        self.cursor.close()
        self.connection.close()

