from dagster import asset, AssetExecutionContext, AssetMaterialization
import pandas as pd
from sqlalchemy import text 
from resources import DbConnectionResource
from sql_loader import get_query

@asset(name="db_connection_test")
def db_connection_test(context, nocodb_db: DbConnectionResource):
    """
    Test asset that uses the configured nocodb_db resource.
    """
    context.log.info(f"Successfully loaded DB resource config.")
    
    # Access attributes as object properties, not dictionary keys
    if nocodb_db.password == "299centblues":
        context.log.info(f"Database: {nocodb_db.database}")
        context.log.info(f"Host: {nocodb_db.host}")
        context.log.info(f"User: {nocodb_db.user}")
        return "SUCCESS: Resource is correctly configured and has the password!"
    else:
        return f"FAILURE: Password mismatch. Got: {nocodb_db.password}"

@asset(name="raw_ing_courant")
def raw_ing_courant(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Source asset: Reads the raw_ing_courant table from PostgreSQL/NocoDB.
    """
    context.log.info("Reading raw_ing_courant table...")
    
    conn = nocodb_db.get_connection()
    try:
        query = "SELECT * FROM raw_ing_courant"
        df = pd.read_sql(query, conn)
        context.log.info(f"Successfully read {len(df)} rows from raw_ing_courant")
        return df
    finally:
        conn.close()


@asset(name="raw_ing_beneficiaries")
def raw_ing_beneficiaries(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Source asset: Reads the raw_ing_beneficiaries table from PostgreSQL/NocoDB.
    """
    context.log.info("Reading raw_ing_beneficiaries table...")
    
    conn = nocodb_db.get_connection()
    try:
        query = "SELECT * FROM raw_ing_beneficiaries"
        df = pd.read_sql(query, conn)
        context.log.info(f"Successfully read {len(df)} rows from raw_ing_beneficiaries")
        return df
    finally:
        conn.close()

@asset(name="gld_ing_courant", deps=["raw_ing_courant", "raw_ing_beneficiaries"])
def gld_ing_courant(context: AssetExecutionContext, nocodb_db: DbConnectionResource):
    """
    Gold layer asset: Performs a FULL OUTER JOIN between raw_ing_courant 
    and raw_ing_beneficiaries on compte_contrepartie.
    Writes the result to gld_ing_courant table in PostgreSQL.
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
        cursor.execute("SELECT COUNT(*) FROM gld_ing_courant")
        row_count = cursor.fetchone()[0]
        context.log.info(f"Successfully created gld_ing_courant with {row_count} rows")
        
        return {"rows_created": row_count, "status": "success"}
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"Error during transformation: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()