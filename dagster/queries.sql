-- SQL transformations for the data pipeline

-- =============================================================================
-- GOLD LAYER: Full Outer Join transformation
-- =============================================================================
-- Source tables are in NocoDB's schema (pxg7tm9k3crofxc)
-- Output table goes to the gold schema
-- =============================================================================

-- name: create_gld_ing_courant
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.gld_ing_courant;

CREATE TABLE gold.gld_ing_courant AS
select
    c.date_comptable,
    c.montant,
    c.devise,
    c.libelles,
    c.details_du_mouvement,
    b.compte_nom,
    b.compte_beneficiaire,
    b.categorie,
    b.reccurence
from pxg7tm9k3crofxc.raw_ing_courant as c
left join pxg7tm9k3crofxc.raw_ing_beneficiaires as b
    on c.compte_contrepartie = b.compte_beneficiaire
    or lower(c.libelles) like '%' || lower(b.libelle_contient) || '%'
        AND b.libelle_contient IS NOT NULL 
        AND b.libelle_contient != ''
order by c.Num_ro_de_mouvement