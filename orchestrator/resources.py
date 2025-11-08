from dagster import ConfigurableResource 
from typing import Any
from pydantic import Field 
import typing

# Define the configuration schema using Pydantic Field
class DbConnectionResource(ConfigurableResource):
    # Use standard Python type hints combined with Pydantic Field
    database: str = Field(description="The name of the database.")
    user: str = Field(description="The database username.")
    password: str = Field(description="The database password.")
    host: str = Field(description="The database host (e.g., 'db').")
    port: int = Field(5432, description="The database port.") # Note: 5432 is the default value
    
    # The actual business logic of the resource goes here
    def get_connection(self):
        # This is where you would connect using psycopg2
        return f"Postgres connection to {self.host}:{self.port} as {self.user}"

    # The actual resource factory function that Dagster calls
    def __call__(self, context) -> Any:
        return self

# You still need to export the class under the old name 
# to ensure your definitions.py import works.
# This line is sometimes necessary depending on Dagster version/setup.
db_connection_resource = DbConnectionResource()
