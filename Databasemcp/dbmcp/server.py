import json
import logging
from typing import Dict, Any
from .factory import DBConnectionFactory

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
