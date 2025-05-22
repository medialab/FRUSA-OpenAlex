/*****************************************************************************
  12. mini_fr_work_sample.sql  –  5 % random sample of French works (+ deps)
******************************************************************************/
-- Purpose
-- -------
-- Quickly spin up a *self‑contained* miniature dataset for local development
-- or teaching demos.  The script:
--   1. Creates a fresh schema (default: `openalex_sample`).
--   2. Clones empty table *skeletons* (structure only) as UNLOGGED tables.
--   3. Samples French‑affiliated works at `:sample_ratio` (default 5 %) using
--      a deterministic hash for reproducibility.
--   4. Pulls in all linked authors, institutions, and metadata via JOIN‑based
--      inserts – so we don’t bring in extraneous rows.
--   5. Creates PK / unique indexes in *parallel* to speed things up.
--   6. ANALYZEs every table and prints sanity counts at the end.
--
-- Notes
-- -----
-- • The hash sampling (`abs(hashtext(wa.work_id)) % 10000`) behaves like a
--   psuedo‑random but stable seed across repeated runs.
-- • All intermediate tables (`tmp_sampled_*`) are UNLOGGED and dropped at
--   commit, keeping WAL usage low.
-- • `%I.%I` format functions are used to inject the dynamic schema name.
-- • `PARALLEL 8` instructs Postgres 15+ to build indexes with eight workers –
--   adjust if your instance has fewer cores.
--


\set ON_ERROR_STOP on
\set sample_ratio 0.05
\set sample_schema openalex_sample
\set S :"sample_schema"

/* fast session knobs */

/* 1. fresh schema */
DROP SCHEMA IF EXISTS :"S" CASCADE;
CREATE SCHEMA :"S";

/* 2. clone skeleton (UNLOGGED) */
WITH core(tab) AS (VALUES
 ('authors'), ('authors_ids'), ('authors_counts_by_year'),
 ('institutions'), ('institutions_geo'), ('institutions_ids'),
 ('works'), ('works_ids'), ('works_authorships'),
 ('works_open_access'), ('works_metadata'),
 ('works_best_oa_locations'), ('works_locations'),
 ('works_primary_locations'), ('works_biblio'),
 ('works_concepts'), ('works_mesh'), ('works_topics'),
 ('works_referenced_works'), ('works_related_works'),
 ('topics'),
 ('concepts'), ('concepts_ancestors'), ('concepts_related_concepts')
)
SELECT format(
  'CREATE UNLOGGED TABLE %I.%I (LIKE openalex.%I INCLUDING ALL);',
  :'S', tab, tab)
FROM core
JOIN pg_tables t ON t.schemaname='openalex' AND t.tablename=core.tab
\gexec

/* 3. sample FR works */
DROP TABLE IF EXISTS tmp_sampled_works;
CREATE UNLOGGED TABLE tmp_sampled_works AS
SELECT DISTINCT wa.work_id
FROM openalex.works_authorships wa
JOIN openalex.institutions_geo ig USING (institution_id)
WHERE ig.country_code='FR'
  AND (abs(hashtext(wa.work_id)::bigint)%10000) <
      (10000*:sample_ratio::numeric);
ALTER TABLE tmp_sampled_works ADD PRIMARY KEY (work_id);
ANALYZE tmp_sampled_works;
SELECT count(*) AS sample_works FROM tmp_sampled_works;

/* 4. authors & institutions */
DROP TABLE IF EXISTS tmp_sampled_authors;
CREATE UNLOGGED TABLE tmp_sampled_authors AS
SELECT DISTINCT wa.author_id
FROM tmp_sampled_works s
JOIN openalex.works_authorships wa USING (work_id);
ALTER TABLE tmp_sampled_authors ADD PRIMARY KEY (author_id);
ANALYZE tmp_sampled_authors;

DROP TABLE IF EXISTS tmp_sampled_institutions;
CREATE UNLOGGED TABLE tmp_sampled_institutions AS
SELECT DISTINCT wa.institution_id
FROM tmp_sampled_works s
JOIN openalex.works_authorships wa USING (work_id)
WHERE wa.institution_id IS NOT NULL;
ALTER TABLE tmp_sampled_institutions ADD PRIMARY KEY (institution_id);
ANALYZE tmp_sampled_institutions;

/* 5. bulk copy (JOIN-driven) */
BEGIN;

INSERT INTO :"S".authors
SELECT a.* FROM tmp_sampled_authors sa
JOIN openalex.authors a ON a.id=sa.author_id;

INSERT INTO :"S".authors_ids
SELECT ai.* FROM tmp_sampled_authors sa
JOIN openalex.authors_ids ai ON ai.author_id=sa.author_id;

INSERT INTO :"S".authors_counts_by_year
SELECT ac.* FROM tmp_sampled_authors sa
JOIN openalex.authors_counts_by_year ac USING (author_id);

INSERT INTO :"S".institutions
SELECT i.* FROM tmp_sampled_institutions si
JOIN openalex.institutions i ON i.id=si.institution_id;

INSERT INTO :"S".institutions_geo
SELECT ig.* FROM tmp_sampled_institutions si
JOIN openalex.institutions_geo ig USING (institution_id);

INSERT INTO :"S".institutions_ids
SELECT ii.* FROM tmp_sampled_institutions si
JOIN openalex.institutions_ids ii USING (institution_id);

INSERT INTO :"S".works
SELECT w.* FROM tmp_sampled_works s
JOIN openalex.works w ON w.id=s.work_id;

INSERT INTO :"S".works_ids
SELECT wi.* FROM tmp_sampled_works s
JOIN openalex.works_ids wi USING (work_id);

INSERT INTO :"S".works_open_access
SELECT woa.* FROM tmp_sampled_works s
JOIN openalex.works_open_access woa USING (work_id);

INSERT INTO :"S".works_metadata
SELECT wm.* FROM tmp_sampled_works s
JOIN openalex.works_metadata wm USING (work_id);

INSERT INTO :"S".works_best_oa_locations
SELECT wbol.* FROM tmp_sampled_works s
JOIN openalex.works_best_oa_locations wbol USING (work_id);

INSERT INTO :"S".works_locations
SELECT wl.* FROM tmp_sampled_works s
JOIN openalex.works_locations wl USING (work_id);

INSERT INTO :"S".works_primary_locations
SELECT wpl.* FROM tmp_sampled_works s
JOIN openalex.works_primary_locations wpl USING (work_id);

INSERT INTO :"S".works_biblio
SELECT wb.* FROM tmp_sampled_works s
JOIN openalex.works_biblio wb USING (work_id);

INSERT INTO :"S".works_concepts
SELECT wc.* FROM tmp_sampled_works s
JOIN openalex.works_concepts wc USING (work_id);

INSERT INTO :"S".works_mesh
SELECT wm.* FROM tmp_sampled_works s
JOIN openalex.works_mesh wm USING (work_id);

INSERT INTO :"S".works_topics
SELECT wt.* FROM tmp_sampled_works s
JOIN openalex.works_topics wt USING (work_id);

INSERT INTO :"S".works_authorships
SELECT wa.* FROM tmp_sampled_works s
JOIN openalex.works_authorships wa USING (work_id)
WHERE wa.author_id IN (SELECT author_id FROM tmp_sampled_authors);

INSERT INTO :"S".works_referenced_works
SELECT wrw.* FROM tmp_sampled_works a
JOIN openalex.works_referenced_works wrw ON wrw.work_id=a.work_id
JOIN tmp_sampled_works b ON b.work_id=wrw.referenced_work_id;

INSERT INTO :"S".works_related_works
SELECT wr.* FROM tmp_sampled_works a
JOIN openalex.works_related_works wr ON wr.work_id=a.work_id
JOIN tmp_sampled_works b ON b.work_id=wr.related_work_id;

INSERT INTO :"S".topics                    SELECT * FROM openalex.topics;
INSERT INTO :"S".concepts                  SELECT * FROM openalex.concepts;
INSERT INTO :"S".concepts_ancestors        SELECT * FROM openalex.concepts_ancestors;
INSERT INTO :"S".concepts_related_concepts SELECT * FROM openalex.concepts_related_concepts;

COMMIT;

/* 6. build PK/unique indexes in parallel */
WITH src AS (
  SELECT indexdef
  FROM pg_indexes
  WHERE schemaname='openalex'
    AND tablename IN ('authors','authors_ids','authors_counts_by_year',
                      'institutions','institutions_geo','institutions_ids',
                      'works','works_ids')
)
SELECT replace(
         replace(indexdef,
                 'CREATE UNIQUE INDEX',
                 'CREATE UNIQUE INDEX PARALLEL 8'),
         ' ON openalex.',
         format(' ON %I.', :'S'))
FROM src
\gexec

/* 7. analyze */
DO $$DECLARE t RECORD;
BEGIN
 FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = :'S' LOOP
   EXECUTE format('ANALYZE %I.%I;', :'S', t.tablename);
 END LOOP;
END$$;

/* 8. sanity counts */
\echo '── sample counts ─────────────────────'
SELECT 'works'        AS table , count(*) FROM :"S".works        UNION ALL
SELECT 'authors'      AS table , count(*) FROM :"S".authors      UNION ALL
SELECT 'institutions' AS table , count(*) FROM :"S".institutions
ORDER  BY 1;
\echo '──────────────────────────────────────'
