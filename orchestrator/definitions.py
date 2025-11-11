# definitions.py
from dagster import Definitions, load_assets_from_modules 
import assets
from resources import DbConnectionResource

DB_PASSWORD_SECRET = "299centblues"

all_assets = load_assets_from_modules([assets])

# -----------------
# 2. DEFINE RESOURCE CONFIGURATION
# -----------------

nocodb_resource_configured = DbConnectionResource.configured({
"database": "nocodb",
    "user": "postgres",
    "password": DB_PASSWORD_SECRET, 
    "host": "db", 
    "port": 5432
})

# -----------------
# 3. DEFINE THE FINAL PLAN
# -----------------
defs = Definitions(
    assets=all_assets,
    resources={
        "nocodb_db": DbConnectionResource, 
    },
)