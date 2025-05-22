/*****************************************************************************
  2.  article_fr_us_metadata.sql
  ---------------------------------------------------------------------------
  Almost identical to the DE–US build but centred on French‑affiliated works.
  Notice the symmetrical naming pattern so both artefacts can be UNIONed or
  compared later without gymnastics.
******************************************************************************/
-- 1) Drop the old table (full rebuild)
DROP TABLE IF EXISTS openalex.article_fr_us_metadata;

-- 2) Create the new FR–US materialised view (implemented as a normal table
--    for speed – but can be switched to MATERIALIZED VIEW if freshness is a
--    bigger concern than performance).
CREATE TABLE openalex.article_fr_us_metadata AS
WITH auth_counts AS (
  SELECT wa.work_id,
         SUM(CASE WHEN ig.country_code = 'FR' THEN 1 ELSE 0 END)         AS cnt_fr,
         SUM(CASE WHEN ig.country_code = 'US' THEN 1 ELSE 0 END)         AS cnt_us,
         SUM(CASE WHEN ig.country_code NOT IN ('FR','US') THEN 1 ELSE 0 END) AS cnt_other,
         COUNT(*)                                                        AS cnt_total
  FROM   openalex.works_authorships wa
  JOIN   openalex.institutions_geo ig ON ig.institution_id = wa.institution_id
  GROUP  BY wa.work_id
),
fr_works AS (
  -- Runs off a cached `temp_fr_works` produced earlier in the pipeline.
  SELECT work_id,
         year AS publication_year
  FROM   temp_fr_works
  WHERE  year > 1990
),
primary_topic AS (
  SELECT work_id,
         topic_id, topic_name,
         subfield_id, subfield_display_name,
         field_id,    field_display_name,
         domain_id,   domain_display_name
  FROM   temp_primary_topic
)
SELECT fw.work_id,
       fw.publication_year,
       ac.cnt_fr,
       ac.cnt_us,
       ac.cnt_other,
       ac.cnt_total,
       ROUND(ac.cnt_fr   ::numeric / NULLIF(ac.cnt_total,0), 3) AS share_fr,
       ROUND(ac.cnt_us   ::numeric / NULLIF(ac.cnt_total,0), 3) AS share_us,
       ROUND(ac.cnt_other::numeric / NULLIF(ac.cnt_total,0), 3) AS share_other,
       pt.topic_id,
       pt.topic_name,
       pt.subfield_id,
       pt.subfield_display_name,
       pt.field_id,
       pt.field_display_name,
       pt.domain_id,
       pt.domain_display_name
FROM   fr_works      fw
JOIN   auth_counts   ac ON ac.work_id = fw.work_id
JOIN   primary_topic pt ON pt.work_id = fw.work_id
WHERE  ac.cnt_us > 0;   -- must have at least one U.S. co‑author

-- 6) Indexes common slicing columns so dashboards remain snappy.
CREATE INDEX ON openalex.article_fr_us_metadata (publication_year);
CREATE INDEX ON openalex.article_fr_us_metadata (share_fr);
CREATE INDEX ON openalex.article_fr_us_metadata (share_us);
CREATE INDEX ON openalex.article_fr_us_metadata (topic_id);
CREATE INDEX ON openalex.article_fr_us_metadata (field_id);
CREATE INDEX ON openalex.article_fr_us_metadata (domain_id);
ANALYZE openalex.article_fr_us_metadata;