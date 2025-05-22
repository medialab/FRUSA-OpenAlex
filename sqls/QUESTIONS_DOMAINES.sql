/*****************************************************************************
  11. fr_collab_author_metrics.sql  –  French collaboration typology & author pairs
******************************************************************************/
-- This module labels every French‑involved paper by collaboration *type* and
-- provides two auxiliary artefacts:
--   • an author ↔ domain "portfolio" count (to gauge expertise breadth)
--   • all unordered author pairs per FR‑inclusive paper (for network graphs)
--
-- ---------------------------------------------------------------------------
-- 1) `fr_collab_publications` – tag each work as Solo FR / FR‑only / Int’l
-- ---------------------------------------------------------------------------
CREATE TABLE openalex.fr_collab_publications AS
SELECT w.id                                AS work_id,
       w.publication_year                  AS year,
       COUNT(DISTINCT wa.author_id)        AS num_authors,
       COUNT(DISTINCT ig.country_code)     AS country_count,
       CASE
         WHEN COUNT(DISTINCT wa.author_id) = 1 THEN 'Solo FR'     -- single author
         WHEN COUNT(DISTINCT ig.country_code) = 1 THEN 'FR-only' -- all authors French
         ELSE 'International'                                    -- at least one non‑FR country
       END AS collab_type
FROM   openalex.works_authorships wa
JOIN   openalex.institutions_geo ig ON ig.institution_id = wa.institution_id
JOIN   openalex.works            w  ON w.id = wa.work_id
GROUP  BY w.id, w.publication_year
HAVING SUM((ig.country_code = 'FR')::int) > 0;  -- ensure at least one French author

-- Index for quick lookup by work_id
ALTER TABLE openalex.fr_collab_publications ADD PRIMARY KEY (work_id);

-- ---------------------------------------------------------------------------
-- 2) `author_domain_portfolio` – cumulative domain counts per author
-- ---------------------------------------------------------------------------
CREATE TABLE openalex.author_domain_portfolio AS
SELECT wa.author_id,
       pt.domain_id,
       pt.domain_display_name,
       COUNT(DISTINCT wa.work_id) AS work_count
FROM   openalex.works_authorships wa
JOIN   openalex.temp_primary_topic pt ON pt.work_id = wa.work_id
GROUP  BY wa.author_id, pt.domain_id, pt.domain_display_name;
CREATE INDEX ON openalex.author_domain_portfolio (author_id);

-- ---------------------------------------------------------------------------
-- 3) `fr_collab_author_pairs` – every unique author pair on a French paper
-- ---------------------------------------------------------------------------
CREATE TABLE openalex.fr_collab_author_pairs AS
SELECT fp.work_id,
       fp.year,
       fp.collab_type,
       a1.author_id AS author1,
       a2.author_id AS author2
FROM   openalex.fr_collab_publications fp
JOIN   openalex.works_authorships a1 ON a1.work_id = fp.work_id
JOIN   openalex.works_authorships a2 ON a2.work_id = fp.work_id
                                    AND a1.author_id < a2.author_id;  -- avoid duplicates & self‑pairs
CREATE INDEX ON openalex.fr_collab_author_pairs (author1);
CREATE INDEX ON openalex.fr_collab_author_pairs (author2);