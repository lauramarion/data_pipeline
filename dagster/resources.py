from dagster import ConfigurableResource 
from typing import Any, Callable
from pydantic import Field 
import psycopg2


class DbConnectionResource(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str
    
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )