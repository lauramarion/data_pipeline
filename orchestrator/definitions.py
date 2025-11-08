# definitions.py
import os
from dagster import Definitions, load_assets_from_modules 
import assets
from resources import db_connection_resource

all_assets = load_assets_from_modules([assets])

password_env_var = os.environ.get("NOCODB_DB_PASSWORD")

if not password_env_var:
    # If this block executes, the environment variable is MISSING.
    raise Exception("CRITICAL: NOCODB_DB_PASSWORD environment variable is NOT set in the container.")

# We only configure the password, which must come from the environment variable.
nocodb_resource_configured = DbConnectionResource.configured({
    "password": password_env_var, 
})


# -----------------
# 2. DEFINE RESOURCE CONFIGURATION
# -----------------

nocodb_resource_configured = DbConnectionResource.configured({
"password": os.environ.get("NOCODB_DB_PASSWORD"),
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