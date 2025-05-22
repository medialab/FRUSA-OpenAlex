/*****************************************************************************
  8.  country_tables_collab.sql  –  US + X multi‑author shares per work
******************************************************************************/
-- Purpose: produce an article‑level table that slices every US‑containing work
-- by its *other* (non‑FR, non‑DE) partner country.  The output is ideal for
-- cross‑country comparison dashboards.

DROP TABLE IF EXISTS openalex.article_us_country_base;
CREATE UNLOGGED TABLE openalex.article_us_country_base AS
WITH auth AS (
  SELECT wa.work_id,
         w.publication_year,
         ig.country_code
  FROM   openalex.works_authorships wa
  JOIN   openalex.works              w  ON w.id = wa.work_id
  JOIN   openalex.institutions_geo   ig ON ig.institution_id = wa.institution_id
), work_totals AS (
  SELECT work_id,
         publication_year,
         SUM((country_code='US')::int) AS cnt_us,
         SUM((country_code='FR')::int) AS cnt_fr,
         COUNT(*)                      AS cnt_total
  FROM   auth
  GROUP  BY work_id, publication_year
), country_counts AS (
  SELECT work_id, publication_year,
         country_code AS target_country,
         COUNT(*) AS cnt_target
  FROM   auth
  WHERE  country_code <> 'FR'
  GROUP  BY work_id, publication_year, country_code
), mapped AS (
  SELECT cc.*, wt.cnt_us, wt.cnt_fr,
         (wt.cnt_total - wt.cnt_us - wt.cnt_fr - cc.cnt_target) AS cnt_other,
         wt.cnt_total
  FROM   country_counts cc
  JOIN   work_totals    wt USING (work_id, publication_year)
  WHERE  cc.target_country NOT IN ('US','DE')
    AND  wt.cnt_us > 0
), final AS (
  SELECT m.*,
         m.cnt_target::float / NULLIF(m.cnt_total,0) AS share_target,
         m.cnt_us    ::float / NULLIF(m.cnt_total,0) AS share_us,
         m.cnt_fr    ::float / NULLIF(m.cnt_total,0) AS share_fr,
         m.cnt_other ::float / NULLIF(m.cnt_total,0) AS share_other,
         (m.cnt_fr > 0) AS has_fr,
         tp.*
  FROM   mapped m
  LEFT   JOIN openalex.temp_primary_topic tp ON tp.work_id = m.work_id
)
SELECT *
FROM   final
ORDER  BY target_country, share_target DESC, cnt_target DESC;
