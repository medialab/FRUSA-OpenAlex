
/*****************************************************************************
  9.  HIN.sql  –  Heterogeneous Information Network edges
******************************************************************************/
-- Converts multiple relational tables into a single edge list stored in
-- `openalex.hin_edges`.  Each step appends one relation type and logs row
-- counts so progress can be monitored.

-- Table structure with minimal indexing (relation, from, to)…
\set ON_ERROR_STOP on

-- 0. Drop & recreate the table
DROP TABLE IF EXISTS openalex.hin_edges;
CREATE TABLE openalex.hin_edges (
  id             SERIAL        PRIMARY KEY,
  relation_name  TEXT          NOT NULL,
  group1         TEXT          NOT NULL,
  entity1_id     TEXT          NOT NULL,
  group2         TEXT          NOT NULL,
  entity2_id     TEXT          NOT NULL,
  value          TEXT,
  ts             TIMESTAMP
);

--------------------------------------------------------------------------------
-- 1) Authorship: author → article
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[1] inserting authorship edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'authorship'                      AS relation_name,
    'author'                          AS group1,
    wa.author_id                      AS entity1_id,
    'article'                         AS group2,
    wa.work_id                        AS entity2_id,
    NULL::text                        AS value,
    w.publication_date::timestamp     AS ts
  FROM openalex.works_authorships wa
  JOIN openalex.works w
    ON wa.work_id = w.id
  WHERE wa.author_id IS NOT NULL
    AND wa.work_id   IS NOT NULL
    AND w.publication_date IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[1] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 2) Author → Institution (use pub_year → Jan 1)
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[2] inserting author_institution edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'author_institution'                                      AS relation_name,
    'author'                                                  AS group1,
    wa.author_id                                              AS entity1_id,
    'institution'                                            AS group2,
    wa.institution_id                                        AS entity2_id,
    NULL::text                                              AS value,
    make_date(w.publication_year,1,1)::timestamp            AS ts
  FROM openalex.works_authorships wa
  JOIN openalex.works w
    ON wa.work_id = w.id
  WHERE wa.author_id     IS NOT NULL
    AND wa.institution_id IS NOT NULL
    AND w.publication_year   IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[2] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 3) Article → Institution
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[3] inserting article_institution edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_institution'                                  AS relation_name,
    'article'                                             AS group1,
    wm.work_id                                            AS entity1_id,
    'institution'                                        AS group2,
    inst_id                                              AS entity2_id,
    NULL::text                                           AS value,
    w.publication_date::timestamp                        AS ts
  FROM openalex.works_metadata wm
  JOIN openalex.works w
    ON wm.work_id = w.id
  CROSS JOIN LATERAL unnest(wm.institution_ids) AS inst_id
  WHERE inst_id             IS NOT NULL
    AND w.publication_date  IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[3] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 4) Article → Country
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[4] inserting article_country edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_country'                                     AS relation_name,
    'article'                                            AS group1,
    wm.work_id                                           AS entity1_id,
    'country'                                           AS group2,
    ccode                                               AS entity2_id,
    NULL::text                                          AS value,
    w.publication_date::timestamp                       AS ts
  FROM openalex.works_metadata wm
  JOIN openalex.works w
    ON wm.work_id = w.id
  CROSS JOIN LATERAL unnest(wm.country_codes) AS ccode
  WHERE ccode             IS NOT NULL
    AND w.publication_date IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[4] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 5) Institution → Country
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[5] inserting institution_country edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'institution_country'                              AS relation_name,
    'institution'                                     AS group1,
    ig.institution_id                                 AS entity1_id,
    'country'                                         AS group2,
    ig.country_code                                   AS entity2_id,
    NULL::text                                        AS value,
    NULL::timestamp                                   AS ts
  FROM openalex.institutions_geo ig
  WHERE ig.institution_id IS NOT NULL
    AND ig.country_code   IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[5] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 6) Article → Topic
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[6] inserting article_topic edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_topic'                                  AS relation_name,
    'article'                                       AS group1,
    wt.work_id                                      AS entity1_id,
    'topic'                                         AS group2,
    wt.topic_id                                     AS entity2_id,
    wt.score::text                                  AS value,
    w.publication_date::timestamp                   AS ts
  FROM openalex.works_topics wt
  JOIN openalex.works w
    ON wt.work_id = w.id
  WHERE wt.topic_id          IS NOT NULL
    AND w.publication_date IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[6] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 7) Article → Domain
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[7] inserting article_domain edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'article_domain'                                  AS relation_name,
    'article'                                       AS group1,
    wm.work_id                                      AS entity1_id,
    'domain'                                        AS group2,
    dom                                             AS entity2_id,
    NULL::text                                      AS value,
    w.publication_date::timestamp                   AS ts
  FROM openalex.works_metadata wm
  JOIN openalex.works w
    ON wm.work_id = w.id
  CROSS JOIN LATERAL unnest(wm.domain_ids) AS dom
  WHERE dom                  IS NOT NULL
    AND w.publication_date   IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[7] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- 8) Citation edges
DO $$
DECLARE cnt INT;
BEGIN
  RAISE NOTICE '[8] inserting citation edges…';
  INSERT INTO openalex.hin_edges (
    relation_name, group1, entity1_id,
    group2,         entity2_id,
    value,          ts
  )
  SELECT
    'citation'                                         AS relation_name,
    'article'                                         AS group1,
    acs.citing_work_id                                AS entity1_id,
    'article'                                         AS group2,
    acs.cited_work_id                                 AS entity2_id,
    acs.citation_order::text                          AS value,
    acs.citing_pub_date::timestamp                    AS ts
  FROM openalex.article_citation_sequence acs
  WHERE acs.citing_work_id   IS NOT NULL
    AND acs.cited_work_id     IS NOT NULL
    AND acs.citing_pub_date   IS NOT NULL
  ;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '[8] done: % rows', cnt;
END
$$ LANGUAGE plpgsql;

-- Finally, add your three indexes:
CREATE INDEX idx_hin_edges_relation   ON openalex.hin_edges(relation_name);
CREATE INDEX idx_hin_edges_g1_e1      ON openalex.hin_edges(group1, entity1_id);
CREATE INDEX idx_hin_edges_g2_e2      ON openalex.hin_edges(group2, entity2_id);

\echo 'All steps completed.'
