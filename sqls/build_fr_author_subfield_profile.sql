\set ON_ERROR_STOP on
SET search_path = openalex, public;

/* ═════════════════════════════════════════════════════════════════╗
   ║  [0]  SIZE‑GUARD – abort if openalex > 4 TiB already          ║
   ╚════════════════════════════════════════════════════════════════╝ */
DO $$
DECLARE
    threshold_bytes bigint := 4 * 1024^4;          -- 4 TiB
    sz              bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(oid))
      INTO sz
      FROM pg_class
     WHERE relnamespace = 'openalex'::regnamespace;

    RAISE NOTICE 'openalex schema size before build = %', pg_size_pretty(sz);

    IF sz > threshold_bytes THEN
        RAISE EXCEPTION '❌  Aborting – current size % exceeds 4 TiB.',
                        pg_size_pretty(sz);
    END IF;
END
$$ LANGUAGE plpgsql;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [1]  WORK → TOP‑3 SUB‑FIELDS (only FRENCH‑affiliated works)   ║
   ╚════════════════════════════════════════════════════════════════╝
   We rely on the helper table `openalex.temp_fr_works`
   (1 row per French‑affiliated work) that exists in your DB snapshot.
   If it disappears in the future, build it with:

     CREATE UNLOGGED TABLE temp_fr_works AS
     SELECT DISTINCT work_id
       FROM works_metadata
      WHERE 'FR' = ANY(country_codes);

*/
\echo ****  [1] building work_top3_subfields  ****
DROP TABLE IF EXISTS work_top3_subfields;

CREATE UNLOGGED TABLE work_top3_subfields AS
WITH ranked AS (
    SELECT wt.work_id,
           t.subfield_id,
           ROW_NUMBER() OVER (PARTITION BY wt.work_id
                              ORDER BY wt.score DESC)              AS rn
      FROM openalex.works_topics     wt
      JOIN openalex.topics           t   ON t.id  = wt.topic_id
      JOIN openalex.temp_fr_works    wfr ON wfr.work_id = wt.work_id
)
SELECT work_id, subfield_id
  FROM ranked
 WHERE rn <= 3;                       -- keep the best three

CREATE INDEX work_top3_subfields_work_idx
        ON work_top3_subfields(work_id);
ANALYZE work_top3_subfields;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [2]  WORKS per (author, year, sub‑field)                      ║
   ╚════════════════════════════════════════════════════════════════╝ */
\echo ****  [2] building author_subfield_in_year  ****
DROP TABLE IF EXISTS author_subfield_in_year;

CREATE UNLOGGED TABLE author_subfield_in_year AS
SELECT  wa.author_id,
        w.publication_year                           AS year,
        s.subfield_id,
        COUNT(*)                                     AS n_works
  FROM openalex.works_authorships    wa
  JOIN openalex.works                w  ON w.id = wa.work_id
  JOIN work_top3_subfields           s  ON s.work_id = w.id
 WHERE w.publication_year IS NOT NULL
 GROUP BY wa.author_id, w.publication_year, s.subfield_id;

CREATE INDEX asi_author_year_idx
        ON author_subfield_in_year(author_id, year);
ANALYZE author_subfield_in_year;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [3]  CUMULATIVE COUNTS up to each publication year            ║
   ╚════════════════════════════════════════════════════════════════╝ */
\echo ****  [3] building author_subfield_cum  ****
DROP TABLE IF EXISTS author_subfield_cum;

CREATE UNLOGGED TABLE author_subfield_cum AS
SELECT  author_id,
        year,
        subfield_id,
        SUM(n_works) OVER (PARTITION BY author_id, subfield_id
                           ORDER BY year)           AS cumulative_works
  FROM  author_subfield_in_year;

CREATE INDEX asc_author_year_idx
        ON author_subfield_cum(author_id, year);
ANALYZE author_subfield_cum;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [4]  FINAL TABLE  openalex.author_subfield_profile            ║
   ║       (sparse JSONB vector with *cumulative* counts)           ║
   ╚════════════════════════════════════════════════════════════════╝ */
\echo ****  [4] creating openalex.author_subfield_profile  ****
DROP TABLE IF EXISTS openalex.author_subfield_profile;

CREATE TABLE openalex.author_subfield_profile AS
SELECT  author_id,
        year,
        jsonb_object_agg(subfield_id::text,
                         cumulative_works)         AS subfield_vector
  FROM  author_subfield_cum
 GROUP BY author_id, year;

ALTER TABLE openalex.author_subfield_profile
      ALTER COLUMN author_id SET NOT NULL;

CREATE UNIQUE INDEX idx_asp_author_year
        ON openalex.author_subfield_profile(author_id, year);
ANALYZE openalex.author_subfield_profile;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [5]  CLEAN‑UP STAGING TABLES                                  ║
   ╚════════════════════════════════════════════════════════════════╝ */
\echo ****  [5] dropping staging tables  ****
DROP TABLE IF EXISTS work_top3_subfields;
DROP TABLE IF EXISTS author_subfield_in_year;
DROP TABLE IF EXISTS author_subfield_cum;


/* ═════════════════════════════════════════════════════════════════╗
   ║  [6]  FINAL SIZE & SUCCESS MESSAGE                             ║
   ╚════════════════════════════════════════════════════════════════╝ */
\echo ****  [6] build finished – reporting size  ****
DO $$
DECLARE
    sz bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(oid))
      INTO sz
      FROM pg_class
     WHERE relnamespace = 'openalex'::regnamespace;

    RAISE NOTICE '✅  author_subfield_profile rebuilt successfully.';
    RAISE NOTICE 'openalex schema size after build = %', pg_size_pretty(sz);
END
$$ LANGUAGE plpgsql;
