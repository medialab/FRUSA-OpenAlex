/*****************************************************************************
  5.  citing_sequences.sql  –  Chronological incoming‑citation list
******************************************************************************/
-- ---------------------------------------------------------------------------
-- Goal: materialise a *rich* per‑citation sequence that carries along both
--       titles and full metadata vectors for cited AND citing papers.  This
--       allows temporal analyses (early vs late adopters) without an expensive
--       join to Works on each query.
-- ---------------------------------------------------------------------------

-- 1) Per‑work metadata cache (array‑typed columns for fast containment filters)
DROP TABLE IF EXISTS openalex.works_metadata;
CREATE TABLE openalex.works_metadata AS
WITH auth AS (
  SELECT wa.work_id,
         wa.institution_id,
         ig.country_code
  FROM   openalex.works_authorships wa
  LEFT   JOIN openalex.institutions_geo ig ON ig.institution_id = wa.institution_id
), topic AS (
  SELECT wt.work_id,
         t.id               AS topic_id,
         t.display_name     AS topic_name,
         t.field_id,
         t.field_display_name    AS field_name,
         t.subfield_id,
         t.subfield_display_name AS subfield_name,
         t.domain_id,
         t.domain_display_name   AS domain_name
  FROM   openalex.works_topics wt
  JOIN   openalex.topics       t  ON t.id = wt.topic_id
)
SELECT w.id AS work_id,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT auth.institution_id), NULL) AS institution_ids,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT auth.country_code),   NULL) AS country_codes,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.topic_id),      NULL) AS topic_ids,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.topic_name),    NULL) AS topic_names,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.field_id),      NULL) AS field_ids,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.field_name),    NULL) AS field_names,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.subfield_id),   NULL) AS subfield_ids,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.subfield_name), NULL) AS subfield_names,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.domain_id),     NULL) AS domain_ids,
       ARRAY_REMOVE(ARRAY_AGG(DISTINCT topic.domain_name),   NULL) AS domain_names
FROM   openalex.works w
LEFT   JOIN auth  ON auth.work_id  = w.id
LEFT   JOIN topic ON topic.work_id = w.id
GROUP  BY w.id;
ALTER TABLE openalex.works_metadata ADD PRIMARY KEY (work_id);

-- 2) Time‑ordered citation sequence with metadata baked‑in
DROP TABLE IF EXISTS openalex.article_citation_sequence;
CREATE TABLE openalex.article_citation_sequence (
  cited_work_id          TEXT  NOT NULL,
  cited_title            TEXT,
  citation_order         INT   NOT NULL,   -- 1 = first citation
  /* cited‑work metadata */
  cited_institution_ids  TEXT[],
  cited_country_codes    TEXT[],
  cited_topic_ids        TEXT[],
  cited_topic_names      TEXT[],
  cited_field_ids        TEXT[],
  cited_field_names      TEXT[],
  cited_subfield_ids     TEXT[],
  cited_subfield_names   TEXT[],
  cited_domain_ids       TEXT[],
  cited_domain_names     TEXT[],
  /* event */
  citing_work_id         TEXT  NOT NULL,
  citing_title           TEXT,
  citing_pub_year        INT,
  citing_pub_date        TEXT,
  /* citing‑work metadata */
  citing_institution_ids TEXT[],
  citing_country_codes   TEXT[],
  citing_topic_ids       TEXT[],
  citing_topic_names     TEXT[],
  citing_field_ids       TEXT[],
  citing_field_names     TEXT[],
  citing_subfield_ids    TEXT[],
  citing_subfield_names  TEXT[],
  citing_domain_ids      TEXT[],
  citing_domain_names    TEXT[],
  PRIMARY KEY (cited_work_id, citation_order)
);

INSERT INTO openalex.article_citation_sequence
SELECT rr.referenced_work_id                   AS cited_work_id,
       w_cited.display_name                    AS cited_title,
       ROW_NUMBER() OVER (
         PARTITION BY rr.referenced_work_id
         ORDER BY w_citing.publication_year NULLS LAST,
                  w_citing.publication_date NULLS LAST)       AS citation_order,
       wm_cited.institution_ids,  wm_cited.country_codes,
       wm_cited.topic_ids,        wm_cited.topic_names,
       wm_cited.field_ids,        wm_cited.field_names,
       wm_cited.subfield_ids,     wm_cited.subfield_names,
       wm_cited.domain_ids,       wm_cited.domain_names,
       rr.work_id                               AS citing_work_id,
       w_citing.display_name                    AS citing_title,
       w_citing.publication_year                AS citing_pub_year,
       w_citing.publication_date                AS citing_pub_date,
       wm_citing.institution_ids, wm_citing.country_codes,
       wm_citing.topic_ids,       wm_citing.topic_names,
       wm_citing.field_ids,       wm_citing.field_names,
       wm_citing.subfield_ids,    wm_citing.subfield_names,
       wm_citing.domain_ids,      wm_citing.domain_names
FROM   openalex.works_referenced_works rr
JOIN   openalex.works            w_citing ON w_citing.id = rr.work_id
JOIN   openalex.works            w_cited  ON w_cited.id  = rr.referenced_work_id
JOIN   openalex.works_metadata   wm_cited ON wm_cited.work_id  = rr.referenced_work_id
JOIN   openalex.works_metadata   wm_citing ON wm_citing.work_id = rr.work_id
ORDER  BY rr.referenced_work_id, citation_order;

-- 3) Index helpers (GIN on array fields for `?` containment ops)
CREATE INDEX ON openalex.article_citation_sequence (cited_work_id);
CREATE INDEX ON openalex.article_citation_sequence (citing_work_id);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (cited_topic_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (cited_field_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (cited_subfield_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (cited_domain_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (citing_topic_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (citing_field_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (citing_subfield_ids);
CREATE INDEX ON openalex.article_citation_sequence USING GIN (citing_domain_ids);