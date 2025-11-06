from dagster import asset, AssetExecutionContext, AssetMaterialization
import pandas as pd
from sqlalchemy import text 
from resources import DbConnectionResource

# Asset 1: Extraction, Staging, and Loading (E & L)
@asset(name="extract_ing_data")
def extract_ing_data(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Pulls both source tables (expenses and beneficiaries) into a single staging table 
    inside the database for transformation.
    """
    engine = nocodb_db.get_engine()
    
    # 1. Pull Expenses Data
    expenses_df = pd.read_sql("SELECT * FROM public.ING_expenses", engine)
    
    # 2. Pull Beneficiaries Data
    beneficiaries_df = pd.read_sql("SELECT * FROM public.ING_beneficiaries", engine)
    
    # 3. Save both dataframes into the same database for easy SQL joining (Staging)
    # Note: We'll create one large staging table here for simplicity
    
    # This example assumes you can join them in Pandas first, or we can stage separately.
    # For a minimal, clean approach, let's stage the expenses and let SQL handle the beneficiaries.
    
    # Simple Staging: Only stage the expenses table for the join (E & L)
    expenses_df.to_sql("stg_ing_expenses", engine, if_exists="replace", index=False) 
    
    context.log.info(f"Staged {len(expenses_df)} rows from ING_expenses.")
    return AssetMaterialization(asset_key="extract_ing_data")


# Asset 2: Transformation (T) - Left Join and Final Table
@asset(
    name="joined_transactions_summary",
    deps=[extract_ing_data] # Runs after the data is staged
)
def joined_transactions_summary(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Performs a LEFT JOIN on staged expenses and the beneficiaries table, 
    then creates the final summary table.
    """
    engine = nocodb_db.get_engine()
    
    # The SQL logic to perform the join and aggregation
    sql_transformation = """
    DROP TABLE IF EXISTS final_joined_transactions;

    CREATE TABLE final_joined_transactions AS
    SELECT
        t1.transaction_id,
        t1.amount,
        t1.date,
        t2.beneficiary_name,
        t2.category  -- Assuming beneficiaries table has a category
    FROM stg_ing_expenses t1
    LEFT JOIN public.ING_beneficiaries t2 
        ON t1.beneficiary_id = t2.beneficiary_id  -- Use your actual join column here
    WHERE t1.amount IS NOT NULL;
    """

    with engine.connect() as connection:
        connection.execute(text(sql_transformation))
        connection.commit()
    
    context.log.info("Successfully created final joined table 'final_joined_transactions'.")
    return AssetMaterialization(asset_key="joined_transactions_summary")