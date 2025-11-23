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
    c.nom_du_compte,
    c.numero_de_mouvement,
    c.date_comptable,
    c.montant,
    case when c.montant > 0 then c.montant else 0 end as montant_income,
    case when c.montant < 0 then ABS(c.montant) else 0 end as montant_spending,
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
order by c.numero_de_mouvement