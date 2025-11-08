from dagster import ConfigurableResource 
from typing import Any
from pydantic import Field 

# Define the configuration schema using Pydantic Field
class DbConnectionResource(ConfigurableResource):
    # Set default values for the database connection parameters
    # This prevents the validation error when using .configured({})
    database: str = "nocodb"
    user: str = "postgres"
    password: str
    host: str = "db"
    port: int = 5432
    
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
