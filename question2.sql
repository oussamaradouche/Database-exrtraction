-- Création d'une vue contenant la population des départements en fonction de l'année
CREATE OR REPLACE VIEW Population_departement AS
SELECT d.id_departement, d.nom_departement, s.annee, SUM(dp.valeur) as population
FROM Departement d
JOIN Commune c ON d.id_departement = c.id_departement
JOIN Donnee_pop dp ON c.id_commune = dp.id_commune
JOIN Stat_pop s ON dp.id_stat_pop = s.id_stat_pop
WHERE s.code_stat LIKE '%\_POP'
GROUP BY d.id_departement, s.annee
ORDER BY d.id_departement;

-- Création d'une vue contenant la population des régions en fonction de l'année
CREATE OR REPLACE VIEW Population_region AS
SELECT r.id_region, r.nom_region, s.annee, SUM(dp.valeur) as population
FROM Region r
JOIN Departement d ON r.id_region = d.id_region
JOIN Commune c ON d.id_departement = c.id_departement
JOIN Donnee_pop dp ON c.id_commune = dp.id_commune
JOIN Stat_pop s ON dp.id_stat_pop = s.id_stat_pop
WHERE s.code_stat LIKE '%\_POP'
GROUP BY r.id_region, s.annee
ORDER BY r.id_region;



-- Suppression des vues
DROP VIEW Population_departement;
DROP VIEW Population_region;