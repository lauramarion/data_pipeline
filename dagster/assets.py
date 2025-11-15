from dagster import asset, AssetExecutionContext
from resources import DbConnectionResource
from sql_loader import get_query
import pandas as pd

# =============================================================================
# PHASE 1: SOURCE ASSETS - Read from NocoDB tables
# =============================================================================

@asset(name="raw_ing_courant")
def raw_ing_courant(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Source asset: Reads the raw_ing_courant table from PostgreSQL/NocoDB.
    """
    context.log.info("Reading raw_ing_courant table...")
    
    conn = nocodb_db.get_connection()
    try:
        query = "SELECT * FROM pxg7tm9k3crofxc.raw_ing_courant"
        df = pd.read_sql(query, conn)
        context.log.info(f"Successfully read {len(df)} rows from raw_ing_courant")
        return df
    finally:
        conn.close()


@asset(name="raw_ing_beneficiaries")
def raw_ing_beneficiaries(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Source asset: Reads the raw_ing_beneficiaries table from PostgreSQL/NocoDB.
    Note: Table name in DB is 'raw_ing_beneficiaires' (with 'e')
    """
    context.log.info("Reading raw_ing_beneficiaries table...")
    
    conn = nocodb_db.get_connection()
    try:
        query = "SELECT * FROM pxg7tm9k3crofxc.raw_ing_beneficiaires"
        df = pd.read_sql(query, conn)
        context.log.info(f"Successfully read {len(df)} rows from raw_ing_beneficiaries")
        return df
    finally:
        conn.close()


# =============================================================================
# PHASE 2: TRANSFORMATION ASSET - Full Outer Join
# =============================================================================

@asset(name="gld_ing_courant", deps=["raw_ing_courant", "raw_ing_beneficiaries"])
def gld_ing_courant(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Gold layer asset: Performs a FULL OUTER JOIN between raw_ing_courant 
    and raw_ing_beneficiaires on compte_contrepartie.
    Writes the result to gold.gld_ing_courant table in PostgreSQL.
    SQL transformation loaded from queries.sql file.
    """
    context.log.info("Starting transformation: FULL OUTER JOIN...")
    
    # Load SQL query from external file
    transform_query = get_query("create_gld_ing_courant")
    context.log.info("Loaded SQL query from queries.sql")
    
    conn = nocodb_db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Execute the transformation query
        context.log.info("Executing transformation query...")
        cursor.execute(transform_query)
        conn.commit()
        
        # Get row count for logging
        cursor.execute("SELECT COUNT(*) FROM gold.gld_ing_courant")
        row_count = cursor.fetchone()[0]
        context.log.info(f"Successfully created gold.gld_ing_courant with {row_count} rows")
        
        return {"rows_created": row_count, "status": "success"}
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"Error during transformation: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()