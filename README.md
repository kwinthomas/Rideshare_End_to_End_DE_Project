# Rideshare Streaming Lakehouse on Azure

This is a project I created to experiment with streaming data, CDC, autoloader, watermarking, etc. Users can book a rideshare through a simple web app, and watch that booking flow all the way to a dashboard within minutes. Each booking publishes a ride event to Azure Event Hub, which handles the stream the same way Kafka would. A Databricks streaming pipeline reads those events continuously, parses them, and unifies them with historical rides and six reference files that Azure Data Factory loads into the lake on a metadata-driven pipeline. The pipeline applies quality expectation and joins each ride to its city, vehicle type, payment method and status. dbt (core) takes that clean table and builds a star schema on top: seven dimensions and an incremental fact table, with tests on every key and every business rule. Power BI reads the star schema directly rather than on a refresh schedule so that the dashboard reflects new rides within a pipeline cycle

---

## Contents

- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [The pipeline](#the-pipeline)
  - [0. The producer](#1-Producer)
  - [1. Ingestion — Azure Data Factory](#2-ingestion--azure-data-factory)
  - [2. Bronze — raw-faithful landing](#3-bronze--raw-faithful-landing)
  - [3. Silver — staging, reference, enriched](#4-silver--staging-reference-enriched)
  - [4. Gold — dbt star schema](#5-gold--dbt-star-schema)
  - [5. Serving — Power BI](#6-serving--power-bi)
- [Data quality](#data-quality)
- [Running it yourself](#running-it-yourself)

---

## Architecture

```
   FastAPI producer                     GitHub (seed files)
   /book · /book/bulk?n=500                    │
          │                                    │
          │ ride events                        │ HTTP
          ▼                                    ▼
  ┌─────────────────────┐          ┌──────────────────────────────┐
  │  Azure Event Hub    │          │  Azure Data Factory          │
  │  rideshare-topic    │          │  pl_ingest_raw_files         │
  │  Kafka endpoint     │          │    Lookup → ForEach → Copy   │
  └──────────┬──────────┘          └───────────────┬──────────────┘
             │                                     │ copy into
             │                                     ▼
             │                          ┌──────────────────────┐
             │                          │  ADLS Gen2 · raw     │
             │                          └───────────┬──────────┘
             │                                      │ Auto Loader
             ▼                                      ▼
┌────────────────────────────────────────────────────────────────┐
│  Azure Databricks · Unity Catalog · Declarative Pipeline       │
│                                                                │
│  BRONZE   rides_raw           Kafka stream, payload as string│
│           bulk_rides, map_*   Auto Loader, schema evolution    │
│              │                                                 │
│              ▼                                                 │
│  SILVER   staging_rides       two append flows into one table  │
│           ref_*               deduplicated reference views     │
│           rides_enriched      stream-static joins, expectations│
└──────────────────────────┬─────────────────────────────────────┘
                           │  Serverless SQL Warehouse
                           ▼
┌────────────────────────────────────────────────────────────────┐
│  dbt Core · gold                                               │
│    7 staging models  →  7 dimensions + fct_rides (incremental) │
└──────────────────────────┬─────────────────────────────────────┘
                           │  star schema
                           ▼
                   ┌───────────────┐
                   │   Power BI    │  
                   └───────────────┘
```

**Stack**

| Layer | Technology |
|---|---|
| Ride book event generation | Python, FastAPI, Faker |
| Streaming ingest | Azure Event Hub (Kafka-compatible endpoint) |
| Batch ingest | Azure Data Factory — Lookup / ForEach / Copy, HTTP linked service |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks — Lakeflow Declarative Pipelines, Structured Streaming, Auto Loader |
| Governance | Unity Catalog — access connector, storage credential, external locations |
| Transformation | PySpark (bronze, silver) · dbt Core with `dbt-databricks` (gold) |
| Query engine | Databricks Serverless SQL Warehouse |
| Visualisation | Power BI Desktop (through Azure VM), composite model |
| Version control | GitHub — ADF Git integration, Databricks Git folders |

---

## Repository layout (Not exhaustive)

```
Rideshare_End_to_End_DE_Project/
├── Ride_book_src/ The booking app
│ ├── api.py 
│ ├── connection.py 
│ ├── data.py 
│ ├── templates/ home.html, confirmation.html
│ └── requirements.txt
├── Raw_Data/  ADF ingests
│ ├── bulk_rides.json
│ ├── map_*.json 
│ └── files_array.json 
├── adf_files/ ADF Git integration root
│ ├── pipeline/ pl_ingest_raw_files
│ ├── dataset/ HTTP binary, ADLS binary, config JSON
│ └── linkedService/ ls_github_http, ls_adls
├── Databricks_Analysis/
│ ├── 00_setup_catalog.ipynb
│ ├── Rough_Analysis.ipynb
│ └── ETL Pipeline/transformations/
│ ├── 01_bronze_eventhub.py
│ ├── 02_bronze_files.py
│ ├── 03_silver_staging.py
│ ├── 04_silver_enriched.py
│ └── 05_silver_reference.py
├── Dbt/
│ └── rideshare_dbt/
│ ├── dbt_project.yml
│ ├── packages.yml
│ ├── profiles.yml.example
│ ├── models/
│ │ ├── staging/ sources.yml + 7 staging models
│ │ └── marts/ marts.yml + 7 dims + fct_rides
│ ├── macros/ 
│ └── tests/
├── powerbi/
│ ├── rideshare_dashboard.pbix
│ └── dax_measures.md
└── README.md
```

---

## The pipeline

### 1. Producer

A small FastAPI app. `/book` generates one ride confirmation and publishes it.
`/book/bulk?n=100` generates a batch, which is how I create enough volume to see the
streaming layer. The Event Hub producer client is created once and reused, and events are put into batches until the service rejects the next one. 

The app.py is the main file to be run on the terminal for being able to book rides through localhost:8000. As mentioned above, you can either book one ride and bulk book n number of rides. I created the bulk booking ride feature primarly for getting a load of data for the Power BI part of this project.

### 2. Ingestion — Azure Data Factory

`rideshareadf` · pipeline `pl_ingest_raw_files`

Seven files one historical ride file and six reference files, are committed to
this repository, giving the pipeline a HTTP endpoint for the linked services. A Lookup reads
`config/files_array.json`, a ForEach iterates it, and a parameterised Copy lands each
file in ADLS. Adding a file is a config edit, not a pipeline change.

### 3. Bronze — raw-faithful landing

`databricks/transformations/01_bronze_eventhub.py`, `02_bronze_files.py`

`rides_raw` reads the Event Hub through its Kafka endpoint. The Kafka stream —
partition, offset, enqueue time — is preserved and the value is cast to string. No
parsing happens here (it is a bronze layer after all). 

The connection string is a secret reference (`{{secrets/rideshare/eventhub-listen}}`)
resolved from pipeline configuration, using a Listen-only policy scoped to the event
hub topic rather than the namespace.

The seven ref tables use Auto Loader, generated from a config dictionary (check the 02_bronze_files.py to get a better understanding).

### 4. Silver — staging, reference, enriched

`03_silver_staging.py`, `04_silver_enriched.py`, `05_silver_reference.py`

**`staging_rides`** is one streaming table fed by two append flows — one from the
Event Hub stream, one from the historical file. Both describe the same business
event, so they belong in one table, and append flows let each source keep its own
checkpoint. I can re-run the backfill without stopping the stream, or reset the
stream without re-reading the backfill. Unioning two streams inside a single flow
gives one shared checkpoint and an all-or-nothing reset.

The JSON schema is declared explicitly rather than inferred from the bulk file.
Inference makes the stream's contract depend on the contents of a file; declaring it
means a producer change fails loudly instead of silently parsing wrong.

Kafka delivery is at-least-once, so a retried send produces a duplicate `ride_id`.
`dropDuplicatesWithinWatermark` on a ten-minute watermark handles that with a bounded
state store rather than holding every id ever seen.

**`ref_*`** are six materialized views, one per reference file, deduplicated
on source file modification time. They are materialized views rather than
streaming tables because reference data is corrected in place and ADF re-lands the
whole file.

Uniqueness is enforced with `expect_or_fail`, using a windowed occurrence count so
the row-level expectation has something real to check. A duplicate key in reference
data would fan out every fact join downstream, so the pipeline should stop rather
than continue.

**`rides_enriched`** joins the ride stream to the reference views and derives the
business columns — speed, fare per mile, tip percentage, distance bands, weekend
flags. The static side is re-read on every micro-batch, so a corrected
city name applies immediately without replaying; no watermark is needed on that side
and no join state accumulates. A stream-stream join would need watermarks on both
sides and would hold unmatched rows waiting for a match that already exists.

All joins are LEFT (Fact / OLTP table being the left most).

### 5. Gold — dbt star schema

`dbt/rideshare_dbt/`

dbt Core runs against a Serverless SQL Warehouse via `dbt-databricks`. Credentials
come from an environment variable; `profiles.yml` is not committed (for obv reasons :)).

Seven dimensions and one fact table sit in the mart. `dim_date` is generated from a `dbt_utils.date_spine` rather than derived from observed ride dates.

**`fct_rides`** is incremental with a merge on `ride_id`. Merging on the natural key makes the model idempotent — run it twice, mid-stream, and the result is identical. This is the trade the
streaming context forces: in a nightly batch you can assume a clean boundary, against
a live stream you cannot.

Surrogate keys are the natural integer ids rather than hashes. Hashing earns its keep
when the natural key is composite, unstable, or when one natural key maps to several
dimension rows. None of that applies to stable integers from a controlled reference
file, and hashes would make the tables harder to debug for no gain.

### 6. Serving — Power BI

`dim_city` is role-playing the fact has both a pickup and a dropoff city key. I
duplicated the query into `dim_city_dropoff`, two active relationships means a user dragging a
city name onto a visual gets an unambiguous result, and I do not have to write two
versions of every geographic measure. 

Few of DAX Measures which were created can be seen the Power BI folder.



---

## Data quality

Quality gates run at three levels, and each catches something the others cannot.

**Pipeline expectations** (silver) — structural failures drop the row
(`expect_all_or_drop`: null ride id, non-positive distance, incoherent timestamps);
suspicious but real records are counted, not discarded (`expect_all`: implausible
surge, out-of-range ratings). The most useful one recomputes the subtotal from the
rate card and flags any drift over five cents. It should always be zero — if it is
not, either the producer's pricing changed or a rate card was edited, both of which I
want to know about and neither of which should discard revenue data.

**dbt schema tests** — uniqueness and not-null on every key, `relationships` on all
eight foreign keys, accepted values on every derived category, and ranges on the
numeric measures.

**dbt singular tests** — May introduce them at a later stage when I revisit this project later on.

---


## Running it yourself

**Prerequisites** — Azure subscription, Python 3.9+, Power BI Desktop (Windows).

```bash
# 1. Azure resources (portal), all in one resource group
#    Event Hubs namespace (Standard tier — Basic has no Kafka surface)
#    Event hub with Send and Listen policies, scoped to the hub not the namespace
#    ADLS Gen2 with hierarchical namespace enabled
#    Data Factory, connected to this repo in Git mode
#    Databricks (Premium) → access connector → Storage Blob Data Contributor

# 2. Producer
cd producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # paste the Send policy connection string
uvicorn api:app --reload      # localhost:8000

# 3. ADF: run pl_ingest_raw_files → seven files land in raw/ingest/

# 4. Databricks
#    Run databricks/00_setup_catalog.sql once
#    databricks secrets create-scope rideshare
#    databricks secrets put-secret rideshare eventhub-listen   # Listen policy
#    Create a declarative pipeline over databricks/transformations/
#    Configuration: eventhub.namespace, eventhub.topic, eventhub.connection_string

# 5. dbt
cd dbt/rideshare_dbt
pip install dbt-core dbt-databricks
cp profiles.yml.example ~/.dbt/profiles.yml
export DBT_DATABRICKS_TOKEN='dapi...'
dbt deps && dbt debug && dbt build

# 6. Power BI Desktop → Get data → Azure Databricks → select the 8 gold tables
#    Set fct_rides to DirectQuery, dimensions to Dual
#    Mark dim_date as a date table, then add the measures from powerbi/dax_measures.md
```

Update storage account names, warehouse HTTP path and the GitHub base URL to match
your own.

**On macOS**, Power BI Desktop is Windows-only. I used a Windows Server VM in the same
subscription over RDP with folder redirection to retrieve the `.pbix`. Make sure to deallocate the
VM from the portal once done (unless you are feeling generous towards Microsoft).

---

## Licence

MIT. All data is synthetically generated; no third-party dataset is redistributed
here.