from dagster import ConfigurableResource, resource, Field, Int, String
from typing import Any

# Define the configuration schema using Dagster's built-in types
class DbConnectionResource(ConfigurableResource):
    database: str = Field(String)
    user: str = Field(String)
    password: str = Field(String)
    host: str = Field(String)
    port: int = Field(Int)
    
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
