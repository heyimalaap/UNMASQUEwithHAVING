from typing import Any, Dict, Iterable, List
from loguru import logger

class IConnection:
    """
        Generic connection interface
    """
    def __init__(self, db_name: str, schema: str, host: str, port: int, user: str, password: str):
        self.db_name = db_name
        self.schema = schema
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.connection = None

    def connect(self):
        pass

    def close(self):
        pass

    def sql(self, query: str, params: Dict[str, Any] = {}, fetch_one=False, dict_cursor=False, execute_only=False) -> Iterable:
        pass

    def table_names(self) -> List[str]:
        pass


import psycopg2
from psycopg2 import extras as psycopg2_extras

class PostgresConnection(IConnection):
    """
        Connection interface for a postgres sql server
    """
    def connect(self):
        self.connection = psycopg2.connect(self._make_connection_str())

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


    def sql(self, query: str, params: Dict[str, Any] = {}, fetch_one=False, dict_cursor=False, execute_only=False):
        if self.connection is None:
            self.connect()

        cursor = self.cursor() if not dict_cursor else self.dict_cursor()
        cursor.execute(query, params)

        result = None
        description = None

        if not execute_only:
            result = cursor.fetchall() if not fetch_one else cursor.fetchone()
            description = cursor.description

        cursor.close()
        return result, description


    def table_names(self) -> List[str]:
        res, _ = self.sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'")
        if res is None:
            logger.error('Failed to fetch table names.')
            raise RuntimeError('Failed to fetch table names.')

        return [x[0] for x in res]


    def _make_connection_str(self):
        return f"dbname={self.db_name} user={self.user} password={self.password} host={self.host} port={self.port}" 

    def cursor(self):
        return self.connection.cursor()

    def dict_cursor(self):
        return self.connection.cursor(cursor_factory=psycopg2_extras.DictCursor)
