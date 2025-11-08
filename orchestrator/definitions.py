# definitions.py
from dagster import Definitions, load_assets_from_modules 
import assets
from resources import DbConnectionResource

all_assets = load_assets_from_modules([assets])

# -----------------
# 2. DEFINE RESOURCE CONFIGURATION
# -----------------

#nocodb_resource_configured = DbConnectionResource.configured({
#"password": os.environ.get("NOCODB_DB_PASSWORD"),
#})

# -----------------
# 3. DEFINE THE FINAL PLAN
# -----------------
defs = Definitions(
    assets=all_assets,
    resources={
        "nocodb_db": DbConnectionResource, 
    },
)