-- queries.sql
-- SQL transformations for the data pipeline

-- =============================================================================
-- GOLD LAYER: Full Outer Join transformation
-- =============================================================================
-- name: create_gld_ing_courant
-- Creates the gld_ing_courant table by performing a FULL OUTER JOIN
-- between raw_ing_courant and raw_ing_beneficiaires
-- Join condition: compte_contrepartie = compte_beneficiaire
-- Source tables are in NocoDB's schema (pxg7tm9k3crofxc)
-- Output table goes to the gold schema for clean organization
-- =============================================================================

-- Create gold schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.gld_ing_courant;

CREATE TABLE gold.gld_ing_courant AS
SELECT 
    c.Date valeur,
    c.Montant,
    c.Devise,
    c.Libellés,
    c.Détails du mouvement,
    b.compte_nom,
    b.compte_beneficiaire,
    b.categorie,
    b.reccurence
FROM pxg7tm9k3crofxc.raw_ing_courant as c
FULL OUTER JOIN pxg7tm9k3crofxc.raw_ing_beneficiaires as b
    ON c.compte_contrepartie = b.compte_beneficiaire;