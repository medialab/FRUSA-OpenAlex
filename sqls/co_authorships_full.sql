/*****************************************************************************
  6.  co_authorship_full.sql  –  Global country‑level collaboration networks
******************************************************************************/
-- Strategy: first flatten authorship into temp table, then compute two edge
-- weights: CN (nominal) and IRP (share‑weighted).  Self‑edges are allowed and
-- can be filtered post‑hoc.

-- 1) Author‑country facts
DROP TABLE IF EXISTS temp_auth_countries;
CREATE UNLOGGED TABLE temp_auth_countries AS
SELECT wa.work_id,
       date_part('year', w.publication_date::date)::int AS year,
       wa.author_id,
       ig.country_code
FROM   openalex.works_authorships wa
JOIN   openalex.works            w  ON w.id = wa.work_id
JOIN   openalex.institutions_geo ig ON ig.institution_id = wa.institution_id;
CREATE INDEX ON temp_auth_countries (work_id);
CREATE INDEX ON temp_auth_countries (year);
CREATE INDEX ON temp_auth_countries (country_code);
ANALYZE  temp_auth_countries;

-- 2) Country–country CN edges per year
DROP TABLE IF EXISTS collab_cn;
CREATE UNLOGGED TABLE collab_cn AS
WITH paper_countries AS (
  SELECT work_id, year, array_agg(DISTINCT country_code) AS countries
  FROM   temp_auth_countries
  GROUP  BY work_id, year
), n_edges AS (
  SELECT pc.year,
         pc.countries[i] AS country_a,
         pc.countries[j] AS country_b
  FROM   paper_countries pc
  JOIN   generate_subscripts(pc.countries,1) i ON TRUE
  JOIN   generate_subscripts(pc.countries,1) j ON j >= i
)
SELECT year, country_a, country_b, COUNT(*) AS weight_cn
FROM   n_edges
GROUP  BY year, country_a, country_b;
CREATE INDEX ON collab_cn (year);
CREATE INDEX ON collab_cn (country_a);
CREATE INDEX ON collab_cn (country_b);

-- 3) IRP edges (share‑weighted)
DROP TABLE IF EXISTS collab_irp;
CREATE UNLOGGED TABLE collab_irp AS
WITH paper_counts AS (
  SELECT work_id, year, COUNT(*) AS total_authors
  FROM   temp_auth_countries
  GROUP  BY work_id, year
), paper_shares AS (
  SELECT tc.work_id,
         tc.year,
         tc.country_code,
         COUNT(*)::float / pc.total_authors AS share
  FROM   temp_auth_countries tc
  JOIN   paper_counts pc USING (work_id, year)
  GROUP  BY tc.work_id, tc.year, tc.country_code, pc.total_authors
), irp_edges AS (
  SELECT ps1.year,
         ps1.country_code AS country_a,
         ps2.country_code AS country_b,
         SUM(ps1.share * ps2.share) AS weight_irp
  FROM   paper_shares ps1
  JOIN   paper_shares ps2 ON ps1.work_id = ps2.work_id
                         AND ps1.year    = ps2.year
                         AND ps2.country_code >= ps1.country_code
  GROUP  BY ps1.year, country_a, country_b
)
SELECT * FROM irp_edges;
CREATE INDEX ON collab_irp (year);
CREATE INDEX ON collab_irp (country_a);
CREATE INDEX ON collab_irp (country_b);