CREATE OR REPLACE VIEW Vue_Population_Region AS
SELECT r.id_region, r.nom_region, sp.annee, SUM(dp.valeur) AS population_totale
FROM Region r
JOIN Departement d ON r.id_region = d.id_region
JOIN Commune c ON d.id_departement = c.id_departement
JOIN Donnee_pop dp ON c.id_commune = dp.id_commune
JOIN Stat_pop sp ON dp.id_stat_pop = sp.id_stat_pop
GROUP BY r.id_region, r.nom_region, sp.annee
ORDER BY r.id_region, sp.annee;


CREATE OR REPLACE VIEW Vue_Population_Departement AS
SELECT d.id_departement, d.nom_departement, sp.annee, SUM(dp.valeur) AS population_totale
FROM Departement d
JOIN Commune c ON d.id_departement = c.id_departement
JOIN Donnee_pop dp ON c.id_commune = dp.id_commune
JOIN Stat_pop sp ON dp.id_stat_pop = sp.id_stat_pop
GROUP BY d.id_departement, d.nom_departement, sp.annee
ORDER BY d.id_departement, sp.annee;