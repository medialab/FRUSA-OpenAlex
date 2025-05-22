-- 1) Helper table: all FR works × author × country × year
DROP TABLE IF EXISTS temp_fr_auth_countries;
CREATE UNLOGGED TABLE temp_fr_auth_countries AS
SELECT
  wa.work_id,
  date_part('year', w.publication_date::date)::int AS year,
  wa.author_id,
  ig.country_code
FROM works_authorships wa
JOIN works w            ON wa.work_id       = w.id
JOIN institutions_geo ig ON wa.institution_id = ig.institution_id
WHERE wa.work_id IN (
  SELECT DISTINCT wa2.work_id
  FROM works_authorships wa2
  JOIN institutions_geo ig2 ON wa2.institution_id = ig2.institution_id
  WHERE ig2.country_code = 'FR'
);
CREATE INDEX ON temp_fr_auth_countries(work_id);
CREATE INDEX ON temp_fr_auth_countries(year);
CREATE INDEX ON temp_fr_auth_countries(country_code);
ANALYZE temp_fr_auth_countries;

-- 2) FR + any-foreign collaboration: Nominal count (CN)
DROP TABLE IF EXISTS fr_collab_cn;
CREATE UNLOGGED TABLE fr_collab_cn AS
WITH fr_any_works AS (
  SELECT DISTINCT work_id, year
  FROM temp_fr_auth_countries
  WHERE country_code <> 'FR'
),
paper_countries AS (
  SELECT work_id, year, array_agg(DISTINCT country_code) AS countries
  FROM temp_fr_auth_countries
  GROUP BY work_id, year
),
nominal_edges AS (
  SELECT
    pc.year,
    pc.countries[i] AS country_a,
    pc.countries[j] AS country_b
  FROM paper_countries pc
  JOIN fr_any_works fa USING (work_id, year)
  JOIN generate_subscripts(pc.countries, 1) AS i ON TRUE
  JOIN generate_subscripts(pc.countries, 1) AS j ON j >= i
)
SELECT year, country_a, country_b, count(*) AS weight_cn
FROM nominal_edges
GROUP BY year, country_a, country_b;
CREATE INDEX ON fr_collab_cn(year);
CREATE INDEX ON fr_collab_cn(country_a);
CREATE INDEX ON fr_collab_cn(country_b);

-- 3) FR + any-foreign collaboration: Implication-Relative-Proportion (IRP)
DROP TABLE IF EXISTS fr_collab_irp;
CREATE UNLOGGED TABLE fr_collab_irp AS
WITH fr_any_works AS (
  SELECT DISTINCT work_id, year
  FROM temp_fr_auth_countries
  WHERE country_code <> 'FR'
),
paper_counts AS (
  SELECT work_id, year, count(*) AS total_authors
  FROM temp_fr_auth_countries
  WHERE work_id IN (SELECT work_id FROM fr_any_works)
  GROUP BY work_id, year
),
paper_shares AS (
  SELECT
    tc.work_id,
    tc.year,
    tc.country_code,
    count(*)::float / pc.total_authors AS share
  FROM temp_fr_auth_countries tc
  JOIN paper_counts pc USING (work_id, year)
  GROUP BY tc.work_id, tc.year, tc.country_code, pc.total_authors
),
irp_edges AS (
  SELECT
    ps1.year,
    ps1.country_code AS country_a,
    ps2.country_code AS country_b,
    sum(ps1.share * ps2.share) AS weight_irp
  FROM paper_shares ps1
  JOIN paper_shares ps2
    ON ps1.work_id = ps2.work_id
   AND ps1.year    = ps2.year
   AND ps2.country_code >= ps1.country_code
  GROUP BY ps1.year, country_a, country_b
)
SELECT * FROM irp_edges;
CREATE INDEX ON fr_collab_irp(year);
CREATE INDEX ON fr_collab_irp(country_a);
CREATE INDEX ON fr_collab_irp(country_b);

-- 4) FR–US subset: Nominal count (CN)
DROP TABLE IF EXISTS fr_us_cn;
CREATE UNLOGGED TABLE fr_us_cn AS
WITH fr_us_works AS (
  SELECT DISTINCT work_id, year
  FROM temp_fr_auth_countries
  WHERE country_code = 'US'
),
paper_countries_us AS (
  SELECT work_id, year, array_agg(DISTINCT country_code) AS countries
  FROM temp_fr_auth_countries
  WHERE work_id IN (SELECT work_id FROM fr_us_works)
  GROUP BY work_id, year
),
nominal_edges_us AS (
  SELECT
    pc.year,
    pc.countries[i] AS country_a,
    pc.countries[j] AS country_b
  FROM paper_countries_us pc
  JOIN generate_subscripts(pc.countries, 1) AS i ON TRUE
  JOIN generate_subscripts(pc.countries, 1) AS j ON j >= i
)
SELECT year, country_a, country_b, count(*) AS weight_cn
FROM nominal_edges_us
GROUP BY year, country_a, country_b;
CREATE INDEX ON fr_us_cn(year);
CREATE INDEX ON fr_us_cn(country_a);
CREATE INDEX ON fr_us_cn(country_b);

-- 5) FR–US subset: Implication-Relative-Proportion (IRP)
DROP TABLE IF EXISTS fr_us_irp;
CREATE UNLOGGED TABLE fr_us_irp AS
WITH fr_us_works AS (
  SELECT DISTINCT work_id, year
  FROM temp_fr_auth_countries
  WHERE country_code = 'US'
),
paper_counts_us AS (
  SELECT work_id, year, count(*) AS total_authors
  FROM temp_fr_auth_countries
  WHERE work_id IN (SELECT work_id FROM fr_us_works)
  GROUP BY work_id, year
),
paper_shares_us AS (
  SELECT
    tc.work_id,
    tc.year,
    tc.country_code,
    count(*)::float / pc.total_authors AS share
  FROM temp_fr_auth_countries tc
  JOIN paper_counts_us pc USING (work_id, year)
  GROUP BY tc.work_id, tc.year, tc.country_code, pc.total_authors
),
irp_edges_us AS (
  SELECT
    ps1.year,
    ps1.country_code AS country_a,
    ps2.country_code AS country_b,
    sum(ps1.share * ps2.share) AS weight_irp
  FROM paper_shares_us ps1
  JOIN paper_shares_us ps2
    ON ps1.work_id = ps2.work_id
   AND ps1.year    = ps2.year
   AND ps2.country_code >= ps1.country_code
  GROUP BY ps1.year, country_a, country_b
)
SELECT * FROM irp_edges_us;
CREATE INDEX ON fr_us_irp(year);
CREATE INDEX ON fr_us_irp(country_a);
CREATE INDEX ON fr_us_irp(country_b);
