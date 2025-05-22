/* ========================================================================
   build_fr_author_subfield_profile_cf.sql
   ------------------------------------------------------------------------
   * One output table only:
       openalex.author_subfield_profile_cf   – carry‑forward cumulative
   * TOP‑3 sub‑fields per French‑affiliated work
   * Fails fast if openalex > 4 TiB
   * All staging tables are UNLOGGED and dropped afterwards
   =======================================================================*/

------------------------------------------------------------
-- [0]  SIZE‑GUARD – abort if schema already > 4 TiB
------------------------------------------------------------
\echo ****  [0]  size‑guard  ****
DO $$
DECLARE
    threshold_bytes bigint := 4 * 1024^4;             -- 4 TiB
    sz              bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(oid))
      INTO sz
      FROM pg_class
     WHERE relnamespace = 'openalex'::regnamespace;

    RAISE NOTICE 'openalex schema size before build = %', pg_size_pretty(sz);

    IF sz > threshold_bytes THEN
        RAISE EXCEPTION '[X]  Aborting – current size % exceeds 4 TiB.',
                         pg_size_pretty(sz);
    END IF;
END
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- Work in the openalex schema
------------------------------------------------------------
SET search_path = openalex, public;

------------------------------------------------------------
-- [1]  WORK  →  TOP‑3 SUB‑FIELDS   (French works only)
------------------------------------------------------------
\echo ****  [1]  building work_top3_subfields  ****

/*  If `temp_fr_works` is missing, build it with:
      CREATE UNLOGGED TABLE temp_fr_works AS
      SELECT DISTINCT work_id
        FROM works_metadata
       WHERE 'FR' = ANY(country_codes);
*/
DROP TABLE IF EXISTS work_top3_subfields;
CREATE UNLOGGED TABLE work_top3_subfields AS
WITH ranked AS (
    SELECT wt.work_id,
           t.subfield_id,
           ROW_NUMBER() OVER (PARTITION BY wt.work_id
                              ORDER BY wt.score DESC) AS rn
      FROM works_topics         wt
      JOIN topics               t   ON t.id = wt.topic_id
      JOIN temp_fr_works        wfr ON wfr.work_id = wt.work_id
)
SELECT work_id, subfield_id
  FROM ranked
 WHERE rn <= 3;

CREATE INDEX work_top3_subfields_work_idx
        ON work_top3_subfields(work_id);
ANALYZE work_top3_subfields;

------------------------------------------------------------
-- [2]  AUTHOR‑SUBFIELD counts per (author, year)
------------------------------------------------------------
\echo ****  [2]  building author_subfield_in_year  ****

DROP TABLE IF EXISTS author_subfield_in_year;
CREATE UNLOGGED TABLE author_subfield_in_year AS
SELECT  wa.author_id,
        w.publication_year                   AS year,
        s.subfield_id,
        COUNT(*)                             AS n_works
  FROM   works_authorships   wa
  JOIN   works               w  ON w.id = wa.work_id
  JOIN   work_top3_subfields s  ON s.work_id = w.id
 WHERE   w.publication_year IS NOT NULL
 GROUP BY wa.author_id, w.publication_year, s.subfield_id;

CREATE INDEX asi_author_year_idx
        ON author_subfield_in_year(author_id, year);
ANALYZE author_subfield_in_year;

------------------------------------------------------------
-- [3]  CUMULATIVE counts up to each year (sparse)
------------------------------------------------------------
\echo ****  [3]  building author_subfield_cum  ****

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

/* -----------------------------------------------------------------
   [4]  FULL CALENDAR GRID  (author × subfield × year)
   -----------------------------------------------------------------*/
\echo ****  [4]  building tmp_author_sf_grid  ****

DROP TABLE IF EXISTS tmp_author_sf_grid;
CREATE UNLOGGED TABLE tmp_author_sf_grid AS
SELECT  d.author_id,
        d.subfield_id,
        y.year
FROM   (
        SELECT DISTINCT author_id, subfield_id
        FROM   author_subfield_cum
       )               d
JOIN    tmp_author_years y
  ON    y.author_id = d.author_id;

CREATE INDEX asg_author_sf_year_idx
    ON tmp_author_sf_grid(author_id, subfield_id, year);
ANALYZE tmp_author_sf_grid;

/* -----------------------------------------------------------------
   [5]  CARRY‑FORWARD cumulative counts (dense matrix)
   -----------------------------------------------------------------*/
\echo ****  [5]  building tmp_author_subfield_dense  ****

DROP TABLE IF EXISTS tmp_author_subfield_dense;
CREATE UNLOGGED TABLE tmp_author_subfield_dense AS
SELECT  g.author_id,
        g.year,
        g.subfield_id,
        MAX(c.cumulative_works)  -- propagates the last non‑NULL value
          OVER (PARTITION BY g.author_id, g.subfield_id
                ORDER BY g.year
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS carry_cum
FROM    tmp_author_sf_grid      g
LEFT    JOIN author_subfield_cum c
       ON  c.author_id    = g.author_id
       AND c.subfield_id  = g.subfield_id
       AND c.year         = g.year;

CREATE INDEX tasd_author_year_idx
        ON tmp_author_subfield_dense(author_id, year);
ANALYZE tmp_author_subfield_dense;

/* -----------------------------------------------------------------
   [6]  FINAL TABLE  author_subfield_profile_cf
   -----------------------------------------------------------------*/
\echo ****  [6]  creating author_subfield_profile_cf  ****

DROP TABLE IF EXISTS openalex.author_subfield_profile_cf;
CREATE TABLE     openalex.author_subfield_profile_cf AS
SELECT  author_id,
        year,
        jsonb_object_agg(subfield_id::text, carry_cum)
            FILTER (WHERE carry_cum > 0)                     -- keep only seen sub‑fields
            AS subfield_vector
FROM    tmp_author_subfield_dense
GROUP   BY author_id, year;

ALTER TABLE openalex.author_subfield_profile_cf
      ALTER COLUMN author_id SET NOT NULL;

CREATE UNIQUE INDEX idx_asp_cf_author_year
        ON openalex.author_subfield_profile_cf(author_id, year);
ANALYZE openalex.author_subfield_profile_cf;


-- [7]  CLEAN‑UP staging tables
------------------------------------------------------------
\echo ****  [7]  dropping staging tables  ****

--DROP TABLE IF EXISTS work_top3_subfields;
--DROP TABLE IF EXISTS author_subfield_in_year;
--DROP TABLE IF EXISTS author_subfield_cum;
--DROP TABLE IF EXISTS tmp_author_years;
--DROP TABLE IF EXISTS tmp_author_subfield_dense;

------------------------------------------------------------
-- [8]  FINAL SIZE REPORT
------------------------------------------------------------
\echo ****  [8]  build finished – reporting size  ****

DO $$
DECLARE
    sz bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(oid))
      INTO sz
      FROM pg_class
     WHERE relnamespace = 'openalex'::regnamespace;

    RAISE NOTICE '[V]  author_subfield_profile_cf rebuilt successfully.';
    RAISE NOTICE 'openalex schema size after build = %', pg_size_pretty(sz);
END
$$ LANGUAGE plpgsql;
