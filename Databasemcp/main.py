import json
import os
import logging
import argparse
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("dbmcp.log"),
        logging.StreamHandler()
    ]
)

class DBConnectionBase:
    """DB 커넥션 추상 클래스"""
    def __init__(self, config: Dict[str, Any], readonly: bool = False):
        self.config = config
        self.conn = None
        self.readonly = readonly

    def connect(self):
        raise NotImplementedError

    def execute(self, query: str):
        if self.readonly and self._is_write_query(query):
            raise PermissionError("readonly 모드에서는 데이터베이스에 쓰기 작업이 허용되지 않습니다.")
        return self._execute(query)

    def _execute(self, query: str):
        raise NotImplementedError

    def _is_write_query(self, query: str) -> bool:
        # INSERT, UPDATE, DELETE, CREATE, DROP, ALTER 등 쓰기 쿼리 차단
        write_keywords = ["insert", "update", "delete", "create", "drop", "alter", "truncate", "replace"]
        q = query.strip().lower()
        return any(q.startswith(kw) for kw in write_keywords)

    def close(self):
        if self.conn:
            try:
                self.conn.close()
                logging.info(f"DB 연결 종료: {self.config.get('type')}")
            except Exception as e:
                logging.warning(f"DB 연결 종료 중 오류: {e}")
            self.conn = None

class PostgresConnection(DBConnectionBase):
    def __init__(self, config, readonly: bool = False):
        super().__init__(config, readonly=readonly)

    def connect(self):
        try:
            import psycopg2
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

class MSSQLConnection(DBConnectionBase):
    def __init__(self, config, readonly: bool = False):
        super().__init__(config, readonly=readonly)

    def connect(self):
        try:
            import pyodbc
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

class DBConnectionFactory:
    """DB 타입에 따라 적절한 커넥션 객체를 반환"""
    @staticmethod
    def create(config: Dict[str, Any], readonly: bool = False):
        db_type = config.get('type')
        if db_type == 'postgresql':
            return PostgresConnection(config, readonly=readonly)
        elif db_type == 'mssql':
            return MSSQLConnection(config, readonly=readonly)
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {db_type}")

class MCPServer:
    def __init__(self, config_path: str, readonly: bool = False):
        self.config_path = config_path
        self.servers = self.load_servers()
        self.connections: Dict[str, Any] = {}
        self.readonly = readonly

    def load_servers(self):
        try:
            with open(self.config_path, 'r') as f:
                servers = json.load(f)
                logging.info(f"서버 설정 파일 로드 성공: {self.config_path}")
                return servers
        except Exception as e:
            logging.error(f"서버 설정 파일 로드 실패: {e}")
            raise

    def get_connection(self, name: str):
        if name not in self.connections:
            config = self.servers.get(name)
            if not config:
                logging.error(f"서버 설정 없음: {name}")
                raise ValueError(f"No config for server: {name}")
            self.connections[name] = DBConnectionFactory.create(config, readonly=self.readonly)
        return self.connections[name]

    def execute_on(self, server_name: str, query: str):
        try:
            conn = self.get_connection(server_name)
            return conn.execute(query)
        except Exception as e:
            logging.error(f"서버({server_name}) 쿼리 실행 실패: {e}")
            raise

    def close_all(self):
        for conn in self.connections.values():
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MCP 서버 실행")
    parser.add_argument('--readonly', action='store_true', help='readonly 모드로 실행 (DB 쓰기 차단)')
    args = parser.parse_args()

    try:
        mcp = MCPServer(config_path=os.path.join(os.path.dirname(__file__), 'servers.json'), readonly=args.readonly)
        print("서버 목록:", list(mcp.servers.keys()))
        # 예시 쿼리 실행 (사용자에 맞게 수정)
        # result = mcp.execute_on('my_postgres', 'SELECT version();')
        # print(result)
    except Exception as e:
        logging.critical(f"MCP 서버 실행 중 치명적 오류: {e}")
    finally:
        if 'mcp' in locals():
            mcp.close_all()
