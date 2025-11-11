from dagster import ConfigurableResource 
from typing import Any, Callable
from pydantic import Field 

# Define the configuration schema using Pydantic Field
class DbConnectionResource(ConfigurableResource):
    # Set default values for the database connection parameters
    database: str = Field(default="nocodb")
    user: str = Field(default="postgres")
    password: str # This is required and has no default
    host: str = Field(default="db")
    port: int = Field(default=5432)
    
    # The actual business logic of the resource goes here
    def get_connection_info(self):
        # This is where you would connect using psycopg2
        return f"Postgres connection to {self.host}:{self.port} as {self.user}"

    # The actual resource factory function that Dagster calls
    #def __call__(self, context) -> Any:
    #    return self

# This line is sometimes necessary depending on Dagster version/setup.
#db_connection_resource = DbConnectionResource()
