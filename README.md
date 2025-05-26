# FRUSA-OpenAlex
> **Scope:** The code assumes a PostgreSQL cluster that already hosts a fully‑loaded `openalex` schema (i.e. the [OpenAlex dump](https://docs.openalex.org/download/openalex-snapshot) has been ingested).

---

## Repository Layout

```text
FRUSA-OpenAlex/
├── sqls/                        # All build scripts (executed with psql -f)
│   ├── article_de_us_metadata.sql
│   ├── article_fr_us_metadata.sql
│   ├── ...
│   └── finalise_vl.sql
├── docs/                       # Rendered copies of this documentation
├── notebooks/                  # Optional exploration / validation notebooks
└── README.md                   # Thin pointer to docs/PROJECT_DOCUMENTATION.md
```


## Script Catalogue

| #  | File                           | Output object(s)                                                | Purpose                                                   |
| -- | ------------------------------ | --------------------------------------------------------------- | --------------------------------------------------------- |
| 01 | `article_de_us_metadata.sql`   | `openalex.article_de_us_metadata`                               | Per‑paper DE & US co‑auth counts, shares, topic hierarchy |
| 02 | `article_fr_us_metadata.sql`   | `openalex.article_fr_us_metadata`                               | Same as 01 for FR ↔ US                                    |
| 03 | `article_openness.sql`         | `openalex.article_openness_fr`                                  | Upstream vs downstream foreignness per FR paper           |
| 04 | `build_fr_team_stats.sql`      | `openalex.author_domain_profile`                                | Cumulative domain vectors for every author                |
| 05 | `citing_sequences.sql`         | `openalex.works_metadata`, `openalex.article_citation_sequence` | Chronological citation list with full metadata            |
| 06 | `co_authorship_full.sql`       | `collab_cn`, `collab_irp`                                       | Global country–country collaboration matrices             |
| 07 | `co_authorships.sql`           | `fr_*` tables                                                   | FR + foreign & FR‑US specific edges (CN + IRP)            |
| 08 | `country_tables_collab.sql`    | `openalex.article_us_country_base`                              | Article‑level US + X country shares                       |
| 09 | `HIN.sql`                      | `openalex.hin_edges`                                            | Heterogeneous Information Network (8 relations)           |
| 10 | `HIN_last_portion.sql`         | `openalex.hin_edges` (append)                                   | Adds topic/domain/citation edges & re‑indexes             |
| 11 | `fr_collab_author_metrics.sql` | `openalex.fr_collab_*`                                          | Publication labels, author portfolios & pairs             |
| 12 | `mini_fr+work_sample.sql`      | `openalex_sample.*` schema                                      | 5 % reproducible French sample with deps                  |
| 13 | `finalise_vl.sql`              | `openalex_sample.vl_author_pub_history`                         | View of author trajectories in sample                     |

---

