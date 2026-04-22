# Steam-AO3-Big-Data-Analytics

Spark Scala analysis on NYU Dataproc that correlates Steam commercial performance against AO3 fanfiction creation volume as a proxy for female audience engagement.

The file documents contribution on the AO3 side in detail. For the Steam side (data ingestion, cleaning, profiling) see Jingjing's sections further down or [her mirror repo](https://github.com/JingjingWang129/Steam-AO3-Big-Data-Analytics).

---

## 1. Project overview

We join two datasets:

- **AO3 data dump** (works + tags CSVs, Feb 2021 release). Used as a proxy for female audience engagement — the platform's user base is predominantly cisgender women (Rouse & Stanfill, 2025).
- **SteamSpy top 10k games** (scraped via the SteamSpy API).

The final analytic bins each matched game into a 2×2 grid (`is_top_rated` ×`is_high_female_engagement`) and reports revenue and fanfiction word-count distributions per bucket. 

---

## 2. Directory layout

```
ana_code/          FinalCode.scala — joined analytic
data_ingest/
  celia/           AO3 dump download + HDFS upload
  jingjing/        SteamSpy API scraper (worm.py)
etl_code/
  celia/           Clean.scala — AO3 works + tags filtering
  jingjing/        Clean.scala — Steam cleaning
profiling_code/
  celia/           CountRecs (raw + cleaned), FirstCode (numerical stats)
  jingjing/        Steam profiling
screenshots/
  celia/           Per-step execution screenshots for my runs
  jingjing/        Jingjing's run screenshots
docs/              Proposal and final slides
```

---

## 3. HDFS paths and data access

All my (Celia's) data lives under `/user/cl7093_nyu_edu/course_project/` on NYU Dataproc. Jingjing's data lives under `/user/jw8191_nyu_edu/`.

| Dataset                         | HDFS path                                                    | Size           | Owner    |
| ------------------------------- | ------------------------------------------------------------ | -------------- | -------- |
| AO3 works (raw)                 | `/user/cl7093_nyu_edu/course_project/input/works-20210226.csv` | ~960 MB        | Celia    |
| AO3 tags (raw)                  | `/user/cl7093_nyu_edu/course_project/input/tags-20210226.csv` | ~...           | Celia    |
| AO3 works (cleaned)             | `/user/cl7093_nyu_edu/course_project/output/works_cleaned/`  | 7,267,422 rows | Celia    |
| AO3 tags (cleaned, Fandom only) | `/user/cl7093_nyu_edu/course_project/output/tags_cleaned/`   | 204,858 rows   | Celia    |
| SteamSpy raw JSONL              | `/user/jw8191_nyu_edu/steamspy_top10000_raw.jsonl`           | ~3.7 MB        | Jingjing |
| SteamSpy cleaned (parquet)      | `/user/jw8191_nyu_edu/steamspy_cleaned`                      | ~10k rows      | Jingjing |
| Final joined analysis           | `/user/jw8191_nyu_edu/final_joined_analysis`                 | combo buckets  | Team     |

**Access grants.** Read access has been provided to the grading accounts used in previous assignments:

```bash
# Run from cl7093_nyu_edu:
hdfs dfs -setfacl -R -m user:pd2672_nyu_edu:r-x /user/cl7093_nyu_edu/course_project
hdfs dfs -setfacl -R -m user:adm209_nyu_edu:r-x /user/cl7093_nyu_edu/course_project
```

Jingjing has granted equivalent access on her paths.

---

## 4. How to run through the AO3 pipeline

Everything is Spark Scala, run via `spark-shell --deploy-mode client -i <script>`. 

### 4.1 Ingest — `data_ingest/celia/`

AO3 publishes selective data dumps periodically. I used the Feb 2021 release.

```bash
# On local machine
# Download the dump manually from:
#   https://archiveofourown.org/admin_posts/18804
# Unpack the tarball, which yields works-20210226.csv and tags-20210226.csv.

# Upload to HDFS (from NYU Dataproc master node, after scp'ing the CSVs):
hdfs dfs -mkdir -p /user/cl7093_nyu_edu/course_project/input
hdfs dfs -put works-20210226.csv /user/cl7093_nyu_edu/course_project/input/
hdfs dfs -put  tags-20210226.csv /user/cl7093_nyu_edu/course_project/input/
```

### 4.2 Profile raw data — `profiling_code/celia/CountRecs.scala`

```bash
spark-shell --deploy-mode client -i ~/course_project/CountRecs.scala
```

Reports total record counts (works: 7,269,693; tags: 14,467,138) and distinct value distributions for `language`, `complete`, `restricted`, tag `type`, tag `canonical`. Screenshot: `screenshots/celia/result_of_CountRecs_and_Clean/01_CountRecs_raw.png`.

### 4.3 ETL — `etl_code/celia/Clean.scala`

Filters out rows with null `creation_date` / `word_count` / `tags`, keeps only the five analytical columns for works, and keeps only `type == "Fandom"` tags with non-null names. Writes CSV to HDFS.

```bash
spark-shell --deploy-mode client -i ~/course_project/Clean.scala
```

Output sizes:

- `works_cleaned`: 7,267,422 rows (dropped ~2,271 nulls)
- `tags_cleaned`: 204,858 rows (kept only Fandom tags out of 14.4M)

Screenshot: `screenshots/celia/result_of_CountRecs_and_Clean/02_Clean_run.png`.

### 4.4 Profile cleaned data — `profiling_code/celia/CountRecs_cleaned.scala`

```bash
spark-shell --deploy-mode client -i ~/course_project/CountRecs_cleaned.scala
```

Sanity checks that the cleaned data matches expected row counts and column distributions. Screenshot: `screenshots/celia/result_of_CountRecs_and_Clean/03_CountRecs_cleaned.png`.

### 4.5 Hive external tables

Registered the cleaned outputs as external Hive tables in my namespace so they're queryable via SQL:

```sql
CREATE EXTERNAL TABLE cl7093_nyu_edu.works_cleaned (
  creation_date STRING,
  language      STRING,
  complete      STRING,
  word_count    STRING,
  tags          STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/cl7093_nyu_edu/course_project/output/works_cleaned'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE cl7093_nyu_edu.tags_cleaned (
  id           STRING,
  name         STRING,
  canonical    STRING,
  cached_count STRING,
  merger_id    STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/cl7093_nyu_edu/course_project/output/tags_cleaned'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Screenshots under `screenshots/celia/result_of_CountRecs_and_Clean/`: `04_hive_create_tags_cleaned.png`, `05_hive_create_works_cleaned.png`, `06_hive_describe_tables.png`.

### 4.6 Numerical statistics — `profiling_code/celia/FirstCode.scala`

Computes mean / median / mode / stddev / min / max for all numeric columns on **both** datasets (Part 1 Steam by Jingjing, Part 2 AO3 by me), then creates two binary feature columns used downstream:

- `is_top_rated` on Steam (`rating > 0.8`)
- `is_long_fic` on AO3 (`word_count > 10000`)

```bash
spark-shell --deploy-mode client -i ~/course_project/FirstCode.scala
```

Key AO3 findings: mean word count 7213.85, median 2130, stddev 22405.2, mode at 100 words (48,396 fics), max 5,078,036 words. 14% of works are "long fics" by my threshold. Screenshots under `screenshots/celia/result_of_FirstCode/` (five screens: Steam numerical stats, followed by AO3 stats).

### 4.7 Final joined analytic — `ana_code/FinalCode.scala`

Joint work with Jingjing. Reads my `works_cleaned` + `tags_cleaned` and her `steamspy_cleaned`, normalizes game names on both sides to handle inconsistencies (parentheses, punctuation, colons), joins on normalized names, then builds the 2×2 combo grid. Writes final dataset as parquet to `/user/jw8191_nyu_edu/final_joined_analysis`.

```bash
spark-shell --deploy-mode client -i ~/course_project/FinalCode.scala
```

See `docs/final_slides.pdf` slides 11–13 for the resulting insights.

---

## 5. Jingjing's Steam pipeline (summary)

Full details in her mirror repo, but the flow is:

1. `data_ingest/jingjing/worm.py` scrapes SteamSpy's `request=all` endpoint over 10 pages (60s sleep between calls) -> `steamspy_top10000_raw.jsonl` uploaded to `/user/jw8191_nyu_edu/`.
2. `profiling_code/jingjing/CountRecs.scala` counts rows and distinct
   values on the raw JSONL.
3. `etl_code/jingjing/Clean.scala` drops rows with nulls, derives`price_usd = price/100`, `owners_mid = (low+high)/2`, `rating = positive/(positive+negative)`, `estimated_revenue = price_usd × owners_mid`; writes parquet to `steamspy_cleaned`.
4. `profiling_code/jingjing/FirstCode.scala` does numerical profiling and creates `is_top_rated`.

---

## 6. Screenshots index

`screenshots/celia/result_of_CountRecs_and_Clean/` — profiling, ETL, and Hive setup:

1. `01_CountRecs_raw.png` — raw works/tags row counts and value distributions
2. `02_Clean_run.png` — Clean.scala run with output row counts
3. `03_CountRecs_cleaned.png` — post-clean profile
4. `04_hive_create_tags_cleaned.png` — Hive external table DDL for tags_cleaned
5. `05_hive_create_works_cleaned.png` — Hive external table DDL for works_cleaned
6. `06_hive_describe_tables.png` — `show tables` + `describe` verification

`screenshots/celia/result_of_FirstCode/` — FirstCode numerical statistics output, split across five screens (Steam first, then AO3).

FinalCode execution was run jointly with Jingjing; see `screenshots/jingjing/result_of_FinalCode/`.

---

## 7. Obstacles and notes

- Free-to-play games (`price = 0`) are excluded from revenue analysis because `estimated_revenue = price × owners_mid` degenerates to 0 and publishers don't disclose per-title revenue for F2P. This is acknowledged in the analysis.
- AO3 data is a proxy, not a gender-disaggregated player count. Steam does not expose demographic splits.
- Game-name matching is fuzzy by design (normalized lowercase, punctuation stripped, optional colon-prefix match). Exact match alone yielded ~300 joins out of ~10k games; the final combined strategy matched 1,004.

---

## 8. Contact

- Celia Liang — cl7093@nyu.edu
- Jingjing Wang — jw8191@nyu.edu
