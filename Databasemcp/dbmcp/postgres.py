import logging
import psycopg2
from .base import DBConnectionBase

class PostgresConnection(DBConnectionBase):
    def __init__(self, config, readonly: bool = False):
        super().__init__(config, readonly=readonly)

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config.get('port', 5432)
            )
            logging.info(f"PostgreSQL 연결 성공: {self.config['host']}:{self.config.get('port', 5432)}")
        except Exception as e:
            logging.error(f"PostgreSQL 연결 실패: {e}")
            raise

    def _execute(self, query: str):
        try:
            if not self.conn:
                self.connect()
            with self.conn.cursor() as cur:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                    logging.info(f"쿼리 실행 성공: {query}")
                    return result
                logging.info(f"쿼리 실행 성공(결과 없음): {query}")
                return None
        except Exception as e:
            logging.error(f"쿼리 실행 실패: {query} | 오류: {e}")
            raise
