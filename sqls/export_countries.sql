\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_it,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_it,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'IT'
) TO 'article_it_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_es,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_es,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'ES'
) TO 'article_es_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_ca,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_ca,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CA'
) TO 'article_ca_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_ch,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_ch,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CH'
) TO 'article_ch_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_be,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_be,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'BE'
) TO 'article_be_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_nl,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_nl,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'NL'
) TO 'article_nl_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_cn,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_cn,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CN'
) TO 'article_cn_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_jp,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_jp,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'JP'
) TO 'article_jp_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_au,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_au,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'AU'
) TO 'article_au_us_metadata.csv' CSV HEADER;

\echo '=== Done: CSVs created in current directory ==='1~\echo '=== Export top-10 collaborator tables via \copy ==='

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_gb,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_gb,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'GB'
) TO 'article_gb_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_it,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_it,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'IT'
) TO 'article_it_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_es,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_es,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'ES'
) TO 'article_es_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_ca,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_ca,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CA'
) TO 'article_ca_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_ch,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_ch,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CH'
) TO 'article_ch_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_be,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_be,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'BE'
) TO 'article_be_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_nl,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_nl,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'NL'
) TO 'article_nl_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_cn,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_cn,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'CN'
) TO 'article_cn_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_jp,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_jp,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'JP'
) TO 'article_jp_us_metadata.csv' CSV HEADER;

\copy (
SELECT
  work_id,
  publication_year,
  cnt_target     AS cnt_au,
  cnt_us,
  cnt_fr,
  cnt_other,
  cnt_total,
  share_target   AS share_au,
  share_us,
  share_fr,
  share_other,
  has_fr,
  topic_id,
  topic_name,
  subfield_id,
  subfield_display_name,
  field_id,
  field_display_name,
  domain_id,
  domain_display_name
FROM openalex.article_us_country_base
WHERE target_country = 'AU'
) TO 'article_au_us_metadata.csv' CSV HEADER;

\echo '=== Done: CSVs created in current directory ==='
