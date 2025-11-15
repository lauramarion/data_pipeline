-- =============================================================================
-- GOLD LAYER: Full Outer Join transformation
-- =============================================================================
-- name: create_gld_ing_courant
-- Creates the gld_ing_courant table by performing a FULL OUTER JOIN
-- between raw_ing_courant and raw_ing_beneficiaires on compte_contrepartie
-- Source tables are in NocoDB's schema (pxg7tm9k3crofxc)
-- Output table goes to the gold schema for clean organization
-- =============================================================================

-- Create gold schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.gld_ing_courant;

CREATE TABLE gold.gld_ing_courant AS
SELECT 
    COALESCE(c.compte_contrepartie, b.compte_contrepartie) as compte_contrepartie,
    c.*,
    b.*
FROM pxg7tm9k3crofxc.raw_ing_courant as c
FULL OUTER JOIN pxg7tm9k3crofxc.raw_ing_beneficiaires as b
    ON c.compte_contrepartie = b.compte_contrepartie;