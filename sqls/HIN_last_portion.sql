/*****************************************************************************
  10. HIN_last_portion.sql  –  Append missing edges & switch to natural PK
******************************************************************************/
-- Drops the SERIAL primary key (to avoid sequence management) and re‑runs the
-- final three appenders (article_topic, article_domain, citation) in case the
-- first run was interrupted.

\set ON_ERROR_STOP on

-- 0) Drop the old SERIAL id + PK constraint so we no longer depend on the sequence
ALTER TABLE openalex.hin_edges
  DROP CONSTRAINT IF EXISTS hin_edges_pkey,
  DROP COLUMN      IF EXISTS id;

--------------------------------------------------------------------------------
-- 6) Article → Topic
DO $$
DECLARE
  cnt INT;
BEGIN
  RAISE NOTICE '[6] inserting article_topic edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_topic'                   AS relation_name,
    'article'                         AS group1,
    wt.work_id                        AS entity1_id,
    'topic'                           AS group2,
    wt.topic_id                       AS entity2_id,
    wt.score::text                    AS value,
    w.publication_date::timestamp     AS ts
  FROM openalex.works_topics wt
  JOIN openalex.works w
    ON wt.work_id = w.id
  WHERE wt.topic_id        IS NOT NULL
    AND w.publication_date IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[6] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 7) Article → Domain
DO $$
DECLARE
  cnt INT;
BEGIN
  RAISE NOTICE '[7] inserting article_domain edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_domain'                  AS relation_name,
    'article'                         AS group1,
    wm.work_id                        AS entity1_id,
    'domain'                          AS group2,
    dom                               AS entity2_id,
    NULL::text                        AS value,
    w.publication_date::timestamp     AS ts
  FROM openalex.works_metadata wm
  JOIN openalex.works w
    ON wm.work_id = w.id
  CROSS JOIN LATERAL unnest(wm.domain_ids) AS dom
  WHERE dom                 IS NOT NULL
    AND w.publication_date IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[7] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 8) Citation edges
DO $$
DECLARE
  cnt INT;
BEGIN
  RAISE NOTICE '[8] inserting citation edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'citation'                        AS relation_name,
    'article'                         AS group1,
    acs.citing_work_id                AS entity1_id,
    'article'                         AS group2,
    acs.cited_work_id                 AS entity2_id,
    acs.citation_order::text          AS value,
    acs.citing_pub_date::timestamp    AS ts
  FROM openalex.article_citation_sequence acs
  WHERE acs.citing_work_id   IS NOT NULL
    AND acs.cited_work_id     IS NOT NULL
    AND acs.citing_pub_date   IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[8] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 9) Re‐create indexes on the newly‐appended table
CREATE INDEX IF NOT EXISTS idx_hin_edges_relation   ON openalex.hin_edges(relation_name);
CREATE INDEX IF NOT EXISTS idx_hin_edges_g1_e1      ON openalex.hin_edges(group1, entity1_id);
CREATE INDEX IF NOT EXISTS idx_hin_edges_g2_e2      ON openalex.hin_edges(group2, entity2_id);

\echo '✅ Steps 6, 7, 8 and indexing completed successfully.'
