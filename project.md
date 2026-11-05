# Project Brief

In this project you are going to build a data pipeline that processes the **Green Taxi Trips** portion of the [NYC Taxi Trip dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

1. Write a script that downloads the data for the first three months of 2025 and stores it in a local staging directory.
2. Write an ETL or ELT pipeline that reads the locally staged files, processes the data, and calculates revenue per day.

**Bonus task, if you have time:**

1. Use Prefect for workflow orchestration.

# How to Approach It

## Step 1: Extract (brief task 1)

> Revisit: data ingestion

- [x] List what the download needs: host, file name per month, months, target folder
- [x] Build each file's link from those pieces
- [x] Request each file and save it to your staging directory
- [x] Decide what happens when you run it again

**Status:** ✅ Done. The script is [`scripts/download_data.py`](scripts/download_data.py). Run it with:

```bash
uv run scripts/download_data.py
```

### Working 1.1: List what the download needs

All four pieces are defined once, in [`src/taxi_pipeline/config.py`](src/taxi_pipeline/config.py). The download code is in [`src/taxi_pipeline/download.py`](src/taxi_pipeline/download.py), and `scripts/download_data.py` only starts it. The pipeline reads the same settings, so the file names and folders can't drift apart:

| Piece | Value |
|---|---|
| Host | `BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"` |
| File name per month | `green_tripdata_{month}.parquet` (for example `green_tripdata_2025-01.parquet`) |
| Months | `MONTHS = ("2025-01", "2025-02", "2025-03")` |
| Target folder | `STAGING_DIR = data/raw/` (relative to the project root, so the script works from any folder) |

### Working 1.2: Build each file's link from those pieces

The script adds the file name for each month to the host:

```python
filename = f"green_tripdata_{month}.parquet"
url = f"{BASE_URL}/{filename}"
```

Result: `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet`

### Working 1.3: Request each file and save it to the staging directory

- It loops over `MONTHS` and downloads each file with `requests.get(url, stream=True, timeout=60)`.
- It downloads in 1 MB pieces, so a large file never has to fit in memory all at once.
- It saves to a temporary `.part` file first and renames it when the download finishes, so a broken download never leaves a half-written Parquet file.
- If a month fails, the script reports it, still tries the other months, and finishes with exit code `1`.

**Result:**

| File | Size | Rows |
|---|---:|---:|
| `green_tripdata_2025-01.parquet` | 1.2 MB | 48,326 |
| `green_tripdata_2025-02.parquet` | 1.1 MB | 46,621 |
| `green_tripdata_2025-03.parquet` | 1.3 MB | 51,539 |
| **Total** | | **146,486** |

### Working 1.4: Decide what happens when you run it again

- **Default:** files already in `data/raw/` are skipped (`[skip]` in the output), so a rerun is fast and doesn't download anything again.
- **`--force`:** downloads every file again (use this if a file looks broken).
- **`--staging-dir PATH`:** saves the files to a different folder.
- `data/raw/` is in `.gitignore`, so the downloaded data never gets committed to git.

## Step 2: Explore

> Revisit: data quality

- [x] Load one file into a notebook (notebooks to experiment only)
- [x] Find the columns you need: when the trip happened and what was paid
- [x] Check them: data types, missing values, value ranges for dates and amounts
- [x] Note what needs cleaning before you can add up revenue per day

**Status:** ✅ Done. The notebook is [`notebooks/explore.ipynb`](notebooks/explore.ipynb). In VS Code, open it, choose the `.venv` kernel, and click **Run All**.

### Working 2.1: Load one file into a notebook

- Loaded `green_tripdata_2025-01.parquet` with `pd.read_parquet()`: **48,326 rows × 21 columns**.
- Later checks load all three files together (**146,486 rows**), because some problems only show up across months.
- The notebook is only for exploring. Nothing in it is part of the pipeline.

### Working 2.2: Find the columns you need

| Question | Column | Notes |
|---|---|---|
| When did the trip happen? | `lpep_pickup_datetime` | Each trip counts on the day it **started** |
| What was paid? | `total_amount` | Fare + tips + tolls + taxes + surcharges. **Chosen as revenue** |
| (alternative) | `fare_amount` | Metered fare only |
| (context) | `payment_type` | 1 Credit card, 2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided |

### Working 2.3: Check them

| Check | Finding |
|---|---|
| **Data types** | `lpep_pickup_datetime` is `datetime64`, amounts are `float64`. Already correct, no converting needed |
| **Missing values** | None in `lpep_pickup_datetime` or `total_amount`. About 1,836 rows per month are missing `payment_type`, `passenger_count` and similar columns, which revenue doesn't need. `ehail_fee` is always empty |
| **Date range** | Earliest pickup is 2024-12-25, latest is 2025-04-01. **11 trips** fall outside Q1 2025 |
| **Amount range** | Min −$473.10, median $18.90, max $642.14 |
| **Negative amounts** | **478** trips: 337 "No charge", 127 "Dispute", 14 "Cash". These are reversals, not revenue |
| **Zero amounts** | **254** trips |
| **Large amounts** | Only 2 over $500. Plausible long trips |

### Working 2.4: Note what needs cleaning

| # | Problem | Rule for Step 3 |
|---|---|---|
| 1 | Pickups outside Jan 1 to Mar 31, 2025 | **Drop** |
| 2 | `total_amount` below 0 | **Drop** |
| 3 | `total_amount` exactly 0 | **Drop** |
| 4 | Missing values in columns revenue doesn't use | **Keep** |
| 5 | Large amounts (up to about $640) | **Keep** |

**Expected effect:** 146,486 raw rows → 11 outside Q1 removed → 732 with amount ≤ 0 removed → **145,743 clean rows**, covering **90 days** (2025-01-01 to 2025-03-31).

## Step 3: Transform and load (brief task 2)

> Revisit: ETL vs. ELT, dbt

- [x] Choose ETL or ELT: transform in Python before loading, or load raw data and transform in SQL
- [x] Read the staged files
- [x] Clean based on what you found in Step 2
- [x] Group by day and sum the revenue
- [x] Write the result to your output location
- [x] Compare row counts before and after cleaning

**Status:** ✅ Done. Run it with:

```bash
uv run scripts/download_data.py   # Step 1: stage the raw files (skips existing ones)
uv run scripts/run_pipeline.py    # Step 3: extract -> transform -> load
uv run pytest                     # run the tests
```

The code is a small package in [`src/taxi_pipeline/`](src/taxi_pipeline/), with one module per stage:

| File | Stage | What it does |
|---|---|---|
| [`config.py`](src/taxi_pipeline/config.py) | shared | URL, months, folders, column names, reporting period |
| [`download.py`](src/taxi_pipeline/download.py) | Step 1 | Downloads the raw files (moved here from the script) |
| [`extract.py`](src/taxi_pipeline/extract.py) | Extract | Reads the staged Parquet files |
| [`transform.py`](src/taxi_pipeline/transform.py) | Transform | Cleans the trips and computes revenue per day |
| [`load.py`](src/taxi_pipeline/load.py) | Load | Writes the result to `data/processed/` |
| [`pipeline.py`](src/taxi_pipeline/pipeline.py) | all | Runs the three stages in order and prints row counts |
| [`scripts/run_pipeline.py`](scripts/run_pipeline.py) | entry point | Starts the pipeline from the command line |
| [`tests/test_taxi_transform.py`](tests/test_taxi_transform.py) | tests | Checks the cleaning rules and daily totals on small hand-made data |

### Working 3.1: Choose ETL or ELT

**Choice: ETL.** The data is transformed in Python (pandas) **before** it's loaded, and only the finished daily table is saved.

- The data is small (about 3.6 MB, 146k rows), so it fits in memory easily.
- There's no database or warehouse in this project. ELT would need one, for example DuckDB or Postgres, to load raw data into and transform with SQL (or dbt).
- pandas is already a project dependency, so no extra tools are needed.
- ELT would be the better fit if the data grew large, or if other people needed to query the raw trips.

### Working 3.2: Read the staged files

- `read_staged()` in `extract.py` builds the three file paths from `config.py`, the same settings the download uses.
- If a file is missing, it stops with a clear message: *"Run `uv run scripts/download_data.py` first."*
- It reads only the 2 columns needed (`lpep_pickup_datetime`, `total_amount`), not all 21, and combines the three months into one table.

### Working 3.3: Clean based on Step 2

`clean_trips()` in `transform.py` applies the two drop rules from Working 2.4:

1. Keep pickups from **2025-01-01 00:00** up to but not including **2025-04-01 00:00**.
2. Keep only trips with `total_amount > 0`.

It returns the clean trips plus a record of how many rows each rule removed.

### Working 3.4: Group by day and sum the revenue

`daily_revenue()` in `transform.py`:

- Groups trips by the **day** of `lpep_pickup_datetime`.
- Calculates `trip_count`, `revenue` (sum of `total_amount`, rounded to cents) and `avg_revenue_per_trip`.
- Makes sure **every day in the period has a row**. A day with no trips would show 0 instead of disappearing.

### Working 3.5: Write the result to the output location

`write_output()` in `load.py` saves two copies in `data/processed/`:

- `daily_revenue.parquet`: for code and later pipeline steps.
- `daily_revenue.csv`: easy to open in Excel or VS Code.

```text
date,trip_count,revenue,avg_revenue_per_trip
2025-01-01,998,25195.01,25.25
2025-01-02,1511,34278.18,22.69
2025-01-03,1521,36533.99,24.02
...
2025-03-31,1682,39594.73,23.54
```

### Working 3.6: Compare row counts before and after cleaning

The pipeline prints this every time it runs:

| Stage | Removed | Rows left |
|---|---:|---:|
| Raw (3 files) | | 146,486 |
| Pickup outside 2025-01-01 .. 2025-03-31 | 11 | 146,475 |
| `total_amount` ≤ 0 | 732 | 145,743 |
| **Clean** | **743 (0.5%)** | **145,743 (99.5% kept)** |

**Result:** 90 daily rows, **$3,401,753.17** total revenue from 145,743 trips. This matches the preview at the end of the Step 2 notebook.

**Known limitation:** negative amounts are reversals of earlier charges (for example, −$473.10 cancels a $473.10 trip). Only the negative row is dropped, so the original charge still counts as revenue. A more exact version would match each reversal to its original trip and drop both.

## Step 4: Orchestrate (bonus)

> Revisit: Prefect

- [x] Get the plain pipeline running end to end first
- [x] Turn each stage into a task and the whole run into a flow
- [x] Add retries where things can fail

**Status:** ✅ Done. The flow is [`src/taxi_pipeline/flow.py`](src/taxi_pipeline/flow.py). Run it with:

```bash
uv run scripts/run_prefect_flow.py
```

**One-time setup:** if Prefect reports `EventsWorker failed` errors, or can't reach `127.0.0.1:4200`, a saved server address from an earlier Prefect project is in the way. Clear it once:

```bash
uv run prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
uv run prefect config unset PREFECT_API_URL --yes   # run this last: `set` can write the old URL back
```

After that, Prefect starts a temporary local server for each run and stops it at the end.

### Working 4.1: Get the plain pipeline running end to end first

The plain version (`scripts/run_pipeline.py`, Step 3) was finished and tested before any Prefect code was written. The flow **reuses the same stage functions** (`download_months`, `read_staged`, `clean_trips`, `daily_revenue`, `write_output`). Prefect only wraps them, so both versions give the same result: **$3,401,753.17 from 145,743 trips**.

### Working 4.2: Turn each stage into a task and the whole run into a flow

| Prefect task | Wraps | Stage |
|---|---|---|
| `download_task` | `download_months()` | Step 1 download |
| `extract_task` | `read_staged()` | Extract |
| `clean_task` | `clean_trips()` | Transform: clean |
| `aggregate_task` | `daily_revenue()` | Transform: group by day |
| `load_task` | `write_output()` | Load |

The flow `green-taxi-daily-revenue` calls the five tasks in order.

- `log_prints=True`: the row counts the stages print appear in Prefect's logs for each task.
- `cache_policy=NO_CACHE` on the tasks that pass DataFrames: Prefect would otherwise try to hash the whole table to cache it, which is slow and not needed for a pipeline this quick.

Example run (shortened):

```text
Task run 'download_task' - [skip] green_tripdata_2025-01.parquet already staged ...
Task run 'extract_task'  - raw rows: 146,486
Task run 'clean_task'    - clean rows: 145,743 of 146,486 (99.5% kept)
Task run 'aggregate_task'- daily rows: 90
Task run 'load_task'     - wrote: daily_revenue.parquet, daily_revenue.csv
Flow run - Finished in state Completed()
```

### Working 4.3: Add retries where things can fail

- **Only `download_task` has retries:** `retries=3, retry_delay_seconds=[5, 15, 30]`. It's the only step that uses the network, so it's the only one that can fail for reasons outside our control. The other steps work on local files; retrying them wouldn't fix anything.
- `download_months()` reports failed months instead of raising, so the task **raises a `RuntimeError`** when any month failed. That's what tells Prefect to retry.
- A retry doesn't download finished files again: months that succeeded are skipped by the "already staged" check from Working 1.4.

**Tested** by requesting a month that doesn't exist (`2099-01`):

```text
[fail] 2099-01: 403 Client Error ... - Retry 1/3 will start 5 second(s) from now
[fail] 2099-01: 403 Client Error ... - Retry 2/3 will start 15 second(s) from now
[fail] 2099-01: 403 Client Error ... - Retry 3/3 will start 30 second(s) from now
[fail] 2099-01: 403 Client Error ... - Retries are exhausted
Flow run - Encountered exception during execution: RuntimeError('download failed for: 2099-01')
```

After the retries run out, the flow stops before extract, so no partial or wrong output is written.
