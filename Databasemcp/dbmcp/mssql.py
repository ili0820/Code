import logging
from .base import DBConnectionBase
import pyodbc

class MSSQLConnection(DBConnectionBase):
    def __init__(self, config, readonly: bool = False):
        super().__init__(config, readonly=readonly)

    def connect(self):
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['host']},{self.config.get('port', 1433)};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['user']};PWD={self.config['password']}"
            )
            self.conn = pyodbc.connect(conn_str)
            logging.info(f"MSSQL 연결 성공: {self.config['host']}:{self.config.get('port', 1433)}")
        except Exception as e:
            logging.error(f"MSSQL 연결 실패: {e}")
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
