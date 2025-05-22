/*****************************************************************************
  4.  build_fr_team_stats.sql
  ---------------------------------------------------------------------------
  Generates cumulative, per‑author domain vectors so we can examine French
  research "teams" or career trajectories.  The script is defensive – it has
  built‑in hard‑size checks to abort automatically when the `openalex` schema
  approaches 3 TB.
******************************************************************************/

/* ---------------------------------------------------------------------------
   CONFIGURATION – pre‑flight safety check.  Running the heavy pipeline only
   makes sense if we are not already at the storage threshold.
--------------------------------------------------------------------------- */
DO $$
DECLARE
    threshold constant bigint := 3 * 1024^4;  -- 3 TB in bytes
    sz        bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(format('%I.%I', n.nspname, c.relname)))
      INTO   sz
    FROM   pg_class      c
    JOIN   pg_namespace  n ON n.oid = c.relnamespace
    WHERE  n.nspname = 'openalex'
      AND  c.relkind  = 'r';

    RAISE NOTICE 'openalex schema size before build = %', pg_size_pretty(sz);

    IF sz > threshold THEN
        RAISE EXCEPTION
          'ABORT: openalex already exceeds 3 TB (%).',
          pg_size_pretty(sz);
    END IF;
END$$ LANGUAGE plpgsql;

/* ---------------------------------------------------------------------------
   1.  work_top3_subfields: weight‑vector with equal weights for the *top‑3*
       sub‑fields (by topic‑score) of each work.  Keeping only three reduces
       cardinality dramatically whilst preserving an author’s profile signal.
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS work_top3_subfields;
CREATE UNLOGGED TABLE work_top3_subfields AS
WITH ranked AS (
    SELECT wt.work_id,
           t.subfield_id,
           ROW_NUMBER() OVER (PARTITION BY wt.work_id ORDER BY wt.score DESC) AS rnk
    FROM   openalex.works_topics wt
    JOIN   openalex.topics       t  ON t.id = wt.topic_id
)
SELECT work_id,
       subfield_id,
       1.0/3 AS weight   -- equal contribution from each of the three sub‑fields
FROM   ranked
WHERE  rnk <= 3
  AND  subfield_id IS NOT NULL;
CREATE INDEX ON work_top3_subfields (work_id);
ANALYZE  work_top3_subfields;
RAISE NOTICE 'Stage‑1 done – % rows', (SELECT COUNT(*) FROM work_top3_subfields);

/* ---------------------------------------------------------------------------
   2.  author_subfield_in_year: aggregate the weighted sub‑field counts per
       author × publication‑year.  This is later turned into a cumulative
       vector so we can see expertise diversification over time.
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS author_subfield_in_year;
CREATE UNLOGGED TABLE author_subfield_in_year AS
SELECT wa.author_id,
       w.publication_year AS year,
       wts.subfield_id,
       SUM(wts.weight)::numeric AS contrib
FROM   openalex.works_authorships wa
JOIN   openalex.works            w   USING (work_id)
JOIN   work_top3_subfields       wts USING (work_id)
GROUP  BY wa.author_id, w.publication_year, wts.subfield_id;
CREATE INDEX ON author_subfield_in_year (author_id, year);
ANALYZE  author_subfield_in_year;
RAISE NOTICE 'Stage‑2 done – % rows', (SELECT COUNT(*) FROM author_subfield_in_year);

/* ---------------------------------------------------------------------------
   3.  author_domain_profile: transform yearly sub‑field counts into JSONB
       vectors where keys are sub‑field IDs and values are *shares* in that
       specific year.  Because we cumulate over time (ROWS BETWEEN UNBOUNDED
       PRECEDING…), the vector in year‑t represents the author’s *total* body
       of work up to that point.
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS openalex.author_domain_profile;
CREATE UNLOGGED TABLE openalex.author_domain_profile AS
WITH cum AS (
    SELECT author_id,
           year,
           subfield_id,
           SUM(contrib) OVER (PARTITION BY author_id, subfield_id ORDER BY year
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_works
    FROM   author_subfield_in_year
), vec AS (
    SELECT author_id,
           year,
           jsonb_object_agg(subfield_id, share) AS domain_vector
    FROM (
        SELECT author_id,
               year,
               subfield_id,
               cum_works,
               cum_works / SUM(cum_works) OVER (PARTITION BY author_id, year) AS share
        FROM   cum
    ) s
    GROUP BY author_id, year
)
SELECT author_id,
       year,
       domain_vector
FROM   vec;
CREATE INDEX idx_adp_author_year ON openalex.author_domain_profile (author_id, year);
ANALYZE  openalex.author_domain_profile;
RAISE NOTICE 'Stage‑3 done – % rows', (SELECT COUNT(*) FROM openalex.author_domain_profile);

/* ---------------------------------------------------------------------------
   4.  Post‑build size verification.  The same guard as at the beginning but
       *after* we have materialised the heavy tables; aborts if the pipeline
       blew past the allowed quota.
--------------------------------------------------------------------------- */
DO $$
DECLARE
    threshold constant bigint := 3 * 1024^4;  -- 3 TB
    sz        bigint;
BEGIN
    SELECT SUM(pg_total_relation_size(format('%I.%I', n.nspname, c.relname)))
      INTO   sz
    FROM   pg_class      c
    JOIN   pg_namespace  n ON n.oid = c.relnamespace
    WHERE  n.nspname = 'openalex'
      AND  c.relkind  = 'r';

    RAISE NOTICE 'openalex schema size after build = %', pg_size_pretty(sz);

    IF sz > threshold THEN
        RAISE EXCEPTION
          'ABORT: schema grew beyond 3 TB (now %).',
          pg_size_pretty(sz);
    END IF;
END$$ LANGUAGE plpgsql;
RAISE NOTICE 'author_domain_profile rebuilt successfully.';
