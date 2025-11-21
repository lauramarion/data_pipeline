-- Install and load PostgreSQL extension
INSTALL postgres;
LOAD postgres;

-- Attach your PostgreSQL database
ATTACH 'host=localhost port=5432 dbname=nocodb user=postgres password=299centblues' 
AS pg (TYPE POSTGRES, READ_ONLY);

-- Create local copies for faster querying
CREATE TABLE raw_ing_courant AS 
SELECT * FROM pg.pxg7tm9k3crofxc.raw_ing_courant;

CREATE TABLE raw_ing_beneficiaires AS 
SELECT * FROM pg.pxg7tm9k3crofxc.raw_ing_beneficiaires;

-- Show what we loaded
SELECT 'raw_ing_courant' as table_name, COUNT(*) as row_count FROM raw_ing_courant
UNION ALL
SELECT 'raw_ing_beneficiaires', COUNT(*) FROM raw_ing_beneficiaires;