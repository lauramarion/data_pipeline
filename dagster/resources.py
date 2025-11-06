from dagster import ConfigurableResource
from sqlalchemy import create_engine
from typing import ClassVar

class DbConnectionResource(ConfigurableResource):
    """
    A resource for creating a SQLAlchemy engine to connect to the database.
    """
    # Connection parameters—Dagster expects these keys to be defined
    host: str       # e.g., 'nocodb' (the Docker container name)
    port: int = 5432
    user: str = "postgres"
    password: str   # Will be provided via an Environment Variable
    database: str = "nocodb"
    
    # This defines the Environment Variable key used to securely pass the password
    env_keys: ClassVar[dict] = {
        "password": "NOCODB_DB_PASSWORD"
    }

    def get_engine(self):
        """Builds and returns the SQLAlchemy engine."""
        db_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(db_url)
