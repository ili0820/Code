import logging
from typing import Dict, Any

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
