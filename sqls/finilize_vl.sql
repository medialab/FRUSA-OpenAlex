/*****************************************************************************
  13. finalise_vl.sql  –  "VL" author publication history in sample schema
******************************************************************************/
-- Context
-- -------
-- "VL" (Very Large?) collaboration data is staged inside the sampled schema
-- from section 12.  This script materialises:
--   • tmp_vl_authors       – all authors who appear in VL_work_collaboration
--   • vl_author_pub_history – per‑author, per‑work time‑series incl. collab
--     type and top‑3 topic IDs.
--   The view is indexed on (author_id) and (work_id) for dashboard filters.
--
-- Implementation tweaks:
--   • Wrapped in a single transaction (BEGIN/COMMIT) so failures roll back.
--   • Uses MATERIALIZED VIEW WITH DATA to persist immediately.
--   • `ARRAY(SELECT … LIMIT 3)` yields a compact top‑3 list without joining
--     back to topics.

\set ON_ERROR_STOP on

BEGIN;

-- 0) (re)materialize the list of sampled authors
DROP TABLE IF EXISTS openalex_sample.tmp_vl_authors;
CREATE UNLOGGED TABLE openalex_sample.tmp_vl_authors AS
SELECT DISTINCT wa.author_id
FROM openalex_sample.works_authorships wa
JOIN openalex_sample.VL_work_collaboration vl USING (work_id);
CREATE INDEX ON openalex_sample.tmp_vl_authors(author_id);


-- 1) build per-author publication history
DROP MATERIALIZED VIEW IF EXISTS openalex_sample.vl_author_pub_history;
CREATE MATERIALIZED VIEW openalex_sample.vl_author_pub_history AS
WITH vl_works AS (
  SELECT work_id, publication_year, collab_type
  FROM openalex_sample.VL_work_collaboration
),
top3 AS (
  SELECT
    wt.work_id,
    ARRAY(
      SELECT topic_id
      FROM openalex_sample.works_topics wt2
      WHERE wt2.work_id = wt.work_id
      ORDER BY wt2.score DESC
      LIMIT 3
    ) AS top3_topics
  FROM (SELECT DISTINCT work_id FROM vl_works) wt
)
SELECT
  wa.author_id,
  vw.work_id,
  vw.publication_year,
  vw.collab_type,
  t3.top3_topics
FROM openalex_sample.works_authorships wa
JOIN openalex_sample.tmp_vl_authors ta
  ON ta.author_id = wa.author_id
JOIN vl_works vw
  ON vw.work_id = wa.work_id
JOIN top3 t3
  ON t3.work_id = vw.work_id
ORDER BY wa.author_id, vw.publication_year, vw.work_id
WITH DATA;

-- 2) index for fast lookups
CREATE INDEX ON openalex_sample.vl_author_pub_history(author_id);
CREATE INDEX ON openalex_sample.vl_author_pub_history(work_id);

COMMIT;
