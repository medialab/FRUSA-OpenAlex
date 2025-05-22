\set ON_ERROR_STOP on

BEGIN;

-- 1) materialize the list of authors in your VL_work_collaboration
DROP TABLE IF EXISTS openalex_sample.tmp_vl_authors;
CREATE UNLOGGED TABLE openalex_sample.tmp_vl_authors AS
WITH vl AS (
  SELECT work_id
  FROM openalex_sample.VL_work_collaboration
)
SELECT DISTINCT wa.author_id
FROM openalex_sample.works_authorships wa
JOIN vl              USING (work_id);

CREATE INDEX ON openalex_sample.tmp_vl_authors(author_id);


-- 2) pull each author’s publications into an unlogged table in openalex_sample
DROP TABLE IF EXISTS openalex_sample.tmp_vl_author_pubs;
CREATE UNLOGGED TABLE openalex_sample.tmp_vl_author_pubs AS
SELECT
  ca.author_id,
  wa.work_id,
  w.publication_year
FROM openalex_sample.works_authorships AS wa
JOIN openalex_sample.works           AS w
  ON w.id = wa.work_id
JOIN openalex_sample.tmp_vl_authors  AS ca
  ON ca.author_id = wa.author_id;

-- now index it (no schema‐prefix mistake)
CREATE INDEX ON openalex_sample.tmp_vl_author_pubs(author_id);
CREATE INDEX ON openalex_sample.tmp_vl_author_pubs(work_id);


-- 3) for each work in VL_work_collaboration, grab the top-3 topics by score
DROP TABLE IF EXISTS openalex_sample.tmp_vl_top3_topics;
CREATE UNLOGGED TABLE openalex_sample.tmp_vl_top3_topics AS
WITH vl AS (
  SELECT work_id
  FROM openalex_sample.VL_work_collaboration
),
ranked AS (
  SELECT
    wt.work_id,
    wt.topic_id,
    wt.score,
    ROW_NUMBER() OVER (PARTITION BY wt.work_id ORDER BY wt.score DESC) AS rn
  FROM openalex_sample.works_topics wt
  JOIN vl ON vl.work_id = wt.work_id
)
SELECT work_id, topic_id, score
FROM ranked
WHERE rn <= 3;

CREATE INDEX ON openalex_sample.tmp_vl_top3_topics(work_id);


COMMIT;

-- quick sanity checks
\echo 'authors involved:'
SELECT count(*) FROM openalex_sample.tmp_vl_authors;

\echo 'total author-publications:'
SELECT count(*) FROM openalex_sample.tmp_vl_author_pubs;

\echo 'distinct works in top3-topics:'
SELECT count(DISTINCT work_id) FROM openalex_sample.tmp_vl_top3_topics;
