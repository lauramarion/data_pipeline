from dagster import asset, AssetExecutionContext, AssetMaterialization
import pandas as pd
from sqlalchemy import text 
from resources import DbConnectionResource

@asset(name="placeholder_asset")
def placeholder_asset():
    """
    A minimal asset to ensure the Code Server can load successfully.
    """
    return "Code server loaded successfully!"

@asset(name="db_connection_test")
def db_connection_test(context, nocodb_db):
    """
    Test asset that uses the configured nocodb_db resource.
    """
    context.log.info(f"Successfully loaded DB resource config.")
    
    # Check if the critical password variable was loaded from the environment
    if nocodb_db["password"] == "299centblues":
        return "SUCCESS: Resource is correctly configured and has the password!"
    else:
        return f"FAILURE: Password mismatch. Got: {nocodb_db['password']}"