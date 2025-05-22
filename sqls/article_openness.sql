/*****************************************************************************
  3.  article_openness.sql   (FR perspective)
  ---------------------------------------------------------------------------
  Calculates *upstream* (references) and *downstream* (citations) openness
  metrics for French works.  A reference/citation is deemed "foreign" when
  *any* author on the other side of the edge is non‑French.
******************************************************************************/
-- ---------------------------------------------------------------------------
-- 1) Mark each referenced work as foreign or FR‑only.  Storing this once in
--    a temp table avoids re‑evaluating BOOL_OR() many times.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_ref_foreign;
CREATE UNLOGGED TABLE temp_ref_foreign AS
SELECT rw.referenced_work_id AS work_id,
       BOOL_OR(ig.country_code <> 'FR') AS is_foreign
FROM   openalex.works_referenced_works rw
JOIN   openalex.works_authorships      wa ON wa.work_id       = rw.referenced_work_id
JOIN   openalex.institutions_geo       ig ON ig.institution_id = wa.institution_id
GROUP  BY rw.referenced_work_id;
CREATE INDEX ON temp_ref_foreign (work_id);
ANALYZE  temp_ref_foreign;

-- ---------------------------------------------------------------------------
-- 2) Upstream statistics – how French are the *references* of each paper?
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_up_stats;
CREATE UNLOGGED TABLE temp_up_stats AS
SELECT fw.work_id,
       COUNT(rw.referenced_work_id)                                              AS up_total_refs,
       COUNT(*) FILTER (WHERE NOT rf.is_foreign)                                 AS up_fr_only_refs,
       COUNT(*) FILTER (WHERE rf.is_foreign)                                     AS up_foreign_refs
FROM   temp_fr_works                fw
JOIN   openalex.works_referenced_works rw ON rw.work_id            = fw.work_id
JOIN   temp_ref_foreign             rf ON rf.work_id               = rw.referenced_work_id
GROUP  BY fw.work_id;
CREATE INDEX ON temp_up_stats (work_id);
ANALYZE  temp_up_stats;

-- ---------------------------------------------------------------------------
-- 3) Repeat the exercise *downstream* – is the citing paper foreign?
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_cite_foreign;
CREATE UNLOGGED TABLE temp_cite_foreign AS
SELECT rw.work_id AS work_id,
       BOOL_OR(ig.country_code <> 'FR') AS is_foreign
FROM   openalex.works_referenced_works rw
JOIN   openalex.works_authorships      wa ON wa.work_id       = rw.work_id
JOIN   openalex.institutions_geo       ig ON ig.institution_id = wa.institution_id
GROUP  BY rw.work_id;
CREATE INDEX ON temp_cite_foreign (work_id);
ANALYZE  temp_cite_foreign;

-- ---------------------------------------------------------------------------
-- 4) Aggregate downstream citation counts per French work.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_down_stats;
CREATE UNLOGGED TABLE temp_down_stats AS
SELECT fw.work_id,
       COUNT(rw.work_id)                                            AS down_total_cites,
       COUNT(*) FILTER (WHERE NOT cf.is_foreign)                    AS down_fr_only_cites,
       COUNT(*) FILTER (WHERE cf.is_foreign)                        AS down_foreign_cites
FROM   temp_fr_works                fw
JOIN   openalex.works_referenced_works rw ON rw.referenced_work_id = fw.work_id
JOIN   temp_cite_foreign            cf ON cf.work_id              = rw.work_id
GROUP  BY fw.work_id;
CREATE INDEX ON temp_down_stats (work_id);
ANALYZE  temp_down_stats;

-- ---------------------------------------------------------------------------
-- 5) (Re)build temp_primary_topic if it is not already loaded by the caller.
--    Placing it inside article_openness.sql makes the script self‑sufficient
--    and saves you from guessing the execution order.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS temp_primary_topic;
CREATE UNLOGGED TABLE temp_primary_topic AS
SELECT x.work_id,
       t.id               AS topic_id,
       t.display_name     AS topic_name,
       t.subfield_id,
       t.subfield_display_name,
       t.field_id,
       t.field_display_name,
       t.domain_id,
       t.domain_display_name
FROM (
  SELECT work_id,
         topic_id,
         ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY score DESC) AS rn
  FROM   openalex.works_topics
) x
JOIN   openalex.topics t ON t.id = x.topic_id
WHERE  x.rn = 1;   -- keep the single best‑scoring topic per work
CREATE INDEX ON temp_primary_topic (work_id);
ANALYZE  temp_primary_topic;

-- ---------------------------------------------------------------------------
-- 6) Finally, merge everything into a permanent table with *both* the
--    openness metrics and the topic hierarchy.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS openalex.article_openness_fr;
CREATE TABLE openalex.article_openness_fr AS
SELECT fw.work_id,
       date_part('year', w.publication_date::date)::int                AS publication_year,
       pt.topic_id,
       pt.topic_name,
       pt.subfield_id,
       pt.subfield_display_name,
       pt.field_id,
       pt.field_display_name,
       pt.domain_id,
       pt.domain_display_name,

       -- upstream refs
       up.up_total_refs,
       up.up_fr_only_refs,
       up.up_foreign_refs,
       ROUND(up.up_fr_only_refs  ::numeric / NULLIF(up.up_total_refs,0), 3) AS up_fr_frac,
       ROUND(up.up_foreign_refs  ::numeric / NULLIF(up.up_total_refs,0), 3) AS up_foreign_frac,

       -- downstream cites
       down.down_total_cites,
       down.down_fr_only_cites,
       down.down_foreign_cites,
       ROUND(down.down_fr_only_cites::numeric / NULLIF(down.down_total_cites,0), 3) AS down_fr_frac,
       ROUND(down.down_foreign_cites::numeric / NULLIF(down.down_total_cites,0), 3) AS down_foreign_frac,

       -- Δ foreign fraction (down‑ vs up‑stream).  Positive → gets cited by
       --         foreigners *more* than it cites them (a form of influence).
       ROUND(
         ROUND(down.down_foreign_cites::numeric / NULLIF(down.down_total_cites,0), 3)
       - ROUND(up.up_foreign_refs  ::numeric / NULLIF(up.up_total_refs,0), 3), 3
       ) AS diff_foreign_frac
FROM   temp_fr_works         fw
JOIN   openalex.works        w    ON w.id          = fw.work_id
LEFT   JOIN temp_primary_topic pt  ON pt.work_id    = fw.work_id
LEFT   JOIN temp_up_stats      up  ON up.work_id    = fw.work_id
LEFT   JOIN temp_down_stats    down ON down.work_id = fw.work_id;

-- Indexes for dashboards / API filters
CREATE INDEX ON openalex.article_openness_fr (publication_year);
CREATE INDEX ON openalex.article_openness_fr (topic_id);
CREATE INDEX ON openalex.article_openness_fr (subfield_id);
CREATE INDEX ON openalex.article_openness_fr (field_id);
CREATE INDEX ON openalex.article_openness_fr (domain_id);