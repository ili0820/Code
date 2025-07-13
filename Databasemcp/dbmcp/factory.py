from typing import Dict, Any
from .postgres import PostgresConnection
from .mssql import MSSQLConnection

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
