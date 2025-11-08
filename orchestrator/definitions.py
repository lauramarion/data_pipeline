# definitions.py
import os
from dagster import Definitions, load_assets_from_modules 
import assets
from resources import db_connection_resource

all_assets = load_assets_from_modules([assets])

# -----------------
# 2. DEFINE RESOURCE CONFIGURATION
# -----------------

nocodb_resource_configured = DbConnectionResource.configured({
    "database": "nocodb",
    "user": "postgres",
    "password": os.environ.get("NOCODB_DB_PASSWORD"), 
    "host": "db", 
    "port": 5432
})

# -----------------
# 3. DEFINE THE FINAL PLAN
# -----------------
defs = Definitions(
    assets=all_assets,
    
    resources={
        "nocodb_db": nocodb_resource_configured,
    },
)