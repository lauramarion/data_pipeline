DROP TABLE IF EXISTS gld_ing_courant;

CREATE TABLE gld_ing_courant AS
SELECT 
    COALESCE(c.compte_contrepartie, b.compte_contrepartie) as compte_contrepartie,
    c.*,
    b.*
FROM raw_ing_courant as c
FULL OUTER JOIN raw_ing_beneficiaries as b
    ON c.compte_contrepartie = b.compte_contrepartie;