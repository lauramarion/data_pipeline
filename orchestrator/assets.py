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