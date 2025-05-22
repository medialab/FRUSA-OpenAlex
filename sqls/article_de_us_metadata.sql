 /***********************************************************************************
 1.  article_de_us_metadata.sql
  ---------------------------------------------------------------------------
  Builds a per‑article metadata table for German–U.S. collaboration papers
  published after 1990.  For every qualifying work we persist:
    • raw author counts by country (DE / US / FR / Other)
    • the corresponding shares (fractions) per paper
    • a boolean flag indicating the presence of *any* French co‑author
    • the primary‑topic hierarchy (topic → sub‑field → field → domain)

  The script is intentionally split into small CTEs so that each conceptual
  step is clear and indexable.
******************************************************************************/
-- ---------------------------------------------------------------------------
-- 0)  Build a *temporary* table with all German‑affiliated works.
--     Keeping it as UNLOGGED + DISTINCT greatly speeds up repeated runs
--     while avoiding bloat in permanent storage.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_de_works;
CREATE UNLOGGED TABLE temp_de_works AS
SELECT DISTINCT
       wa.work_id,
       date_part('year', w.publication_date::date)::int AS publication_year
FROM   openalex.works_authorships wa
JOIN   openalex.institutions_geo ig ON ig.institution_id = wa.institution_id
JOIN   openalex.works            w  ON w.id             = wa.work_id
WHERE  ig.country_code = 'DE';      -- German affiliation only

-- Helpful narrow indexes for later joins / filters
CREATE INDEX ON temp_de_works (work_id);
CREATE INDEX ON temp_de_works (publication_year);
ANALYZE temp_de_works;              -- allow planner to pick the indexes

-- ---------------------------------------------------------------------------
-- 1)  Drop the previous materialised table (if any) so we can rebuild from
--     scratch.  UNLOGGED saves WAL space – acceptable because we can always
--     regenerate this table.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS openalex.article_de_us_metadata;

-- ---------------------------------------------------------------------------
-- 2)  Construct the new DE–US metadata table.
-- ---------------------------------------------------------------------------
CREATE UNLOGGED TABLE openalex.article_de_us_metadata AS
WITH auth_counts AS (
  /* ---------------------------------------------------------------
     Count the number of authors per country for *each* work.
     This CTE purposefully does *not* apply the post‑1990 filter –
     it is country‑aggregated metadata that can be reused elsewhere.
  ---------------------------------------------------------------- */
  SELECT
    wa.work_id,
    SUM(CASE WHEN ig.country_code = 'DE'  THEN 1 ELSE 0 END) AS cnt_de,
    SUM(CASE WHEN ig.country_code = 'US'  THEN 1 ELSE 0 END) AS cnt_us,
    SUM(CASE WHEN ig.country_code = 'FR'  THEN 1 ELSE 0 END) AS cnt_fr,
    SUM(CASE WHEN ig.country_code NOT IN ('DE','US','FR') THEN 1 ELSE 0 END) AS cnt_other,
    COUNT(*)                                                 AS cnt_total
  FROM   openalex.works_authorships wa
  JOIN   openalex.institutions_geo  ig ON ig.institution_id = wa.institution_id
  GROUP  BY wa.work_id
),
primary_topic AS (
  /* ---------------------------------------------------------------
     The hierarchy for the *primary* topic of every work has already
     been isolated upstream (temp_primary_topic).  Re‑selecting here
     keeps the query self‑contained and avoids hard‑coding the schema
     elsewhere.
  ---------------------------------------------------------------- */
  SELECT work_id,
         topic_id, topic_name,
         subfield_id, subfield_display_name,
         field_id,    field_display_name,
         domain_id,   domain_display_name
  FROM   openalex.temp_primary_topic
),
de_works AS (
  /* ---------------------------------------------------------------
     Restrict to German‑affiliated works published *after* 1990.  The
     date cut‑off matches the FR logic so downstream analyses are
     comparable.
  ---------------------------------------------------------------- */
  SELECT work_id, publication_year
  FROM   temp_de_works
  WHERE  publication_year > 1990
)
SELECT
  dw.work_id,
  dw.publication_year,

  -- raw author counts
  ac.cnt_de,
  ac.cnt_us,
  ac.cnt_fr,
  ac.cnt_other,
  ac.cnt_total,

  -- proportional shares (rounded to 3 decimals)
  ROUND(ac.cnt_de   ::numeric / NULLIF(ac.cnt_total,0), 3) AS share_de,
  ROUND(ac.cnt_us   ::numeric / NULLIF(ac.cnt_total,0), 3) AS share_us,
  ROUND(ac.cnt_fr   ::numeric / NULLIF(ac.cnt_total,0), 3) AS share_fr,
  ROUND(ac.cnt_other::numeric / NULLIF(ac.cnt_total,0), 3) AS share_other,

  -- simple boolean for down‑stream convenience (no need to compare >0 later)
  (ac.cnt_fr > 0) AS has_fr,

  -- topic hierarchy passthrough
  pt.topic_id,
  pt.topic_name,
  pt.subfield_id,
  pt.subfield_display_name,
  pt.field_id,
  pt.field_display_name,
  pt.domain_id,
  pt.domain_display_name
FROM   de_works       dw
JOIN   auth_counts    ac ON ac.work_id = dw.work_id
JOIN   primary_topic  pt ON pt.work_id = dw.work_id
WHERE  ac.cnt_de > 0        -- must contain at least one German author
  AND  ac.cnt_us > 0;       -- …and at least one U.S. author

-- ---------------------------------------------------------------------------
-- 3)  Index the main filter columns.  Always create *after* the bulk insert –
--     otherwise Postgres would maintain the index during COPY which hurts I/O.
-- ---------------------------------------------------------------------------
CREATE INDEX ON openalex.article_de_us_metadata (publication_year);
CREATE INDEX ON openalex.article_de_us_metadata (cnt_de);
CREATE INDEX ON openalex.article_de_us_metadata (cnt_us);
CREATE INDEX ON openalex.article_de_us_metadata (has_fr);
ANALYZE openalex.article_de_us_metadata;