# definitions.py

from dagster import Definitions, load_assets_from_modules # ResourceDefinition is no longer needed
import assets 
from resources import DbConnectionResource 

# -----------------
# 1. LOAD ASSETS
# -----------------
all_assets = load_assets_from_modules([assets])

# -----------------
# 2. DEFINE RESOURCE CONFIGURATION (Final Compatible Fix)
# -----------------
# We create the resource instance directly. Because DbConnectionResource
# inherits from ConfigurableResource and contains the 'env_keys' mapping in
# resources.py, Dagster automatically handles the NOCODB_DB_PASSWORD lookup 
# at the appropriate time (the previous crashing was a loading fluke).
nocodb_resource_configured = DbConnectionResource(
    host="nocodb",
    # The password will be injected by Dagster from the environment variable 
    # based on the 'env_keys' map in resources.py.
)

# -----------------
# 3. DEFINE THE FINAL PLAN
# -----------------
defs = Definitions(
    assets=all_assets,
    
    resources={
        "nocodb_db": nocodb_resource_configured,
    },
)