# Data Pipeline Project

Use this repository as a **template** for your data pipeline project. You will build a pipeline that takes raw NYC taxi trip records and turns them into a processed result. The project is open-ended: the brief sets the goal, and the structure, tools, and implementation choices are yours to make. Create pull requests in your own copy even if you are working alone, and use them to track your progress.

## Learning Objectives

By the end of this repository, you should be able to:

- Write a script that downloads a public dataset into a local staging directory.
- Build a pipeline that reads staged Parquet files and aggregates them into daily metrics.
- Separate a pipeline into distinct extract, transform, and load stages.
- Explain the difference between ETL and ELT, and justify which one your pipeline uses.
- Orchestrate a pipeline with Prefect (stretch goal).

## Project Brief

In this project you are going to build a data pipeline that processes the `Green Taxi Trips` portion of the [NYC Taxi Trip dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

1. Write a script that downloads the data for the first three months of 2025 and stores it in a local staging directory.
2. Write an ETL or ELT pipeline that reads the locally staged files, processes the data, and calculates revenue per day.

Bonus task, if you have time:

1. Use Prefect for workflow orchestration.

## Learning Path

### Additional Folders and Files

| File / Folder | Description |
|---|---|
| [**solution**](solution/) | A complete local ETL implementation with its own README, scripts, package, and tests. |
| [**pyproject.toml**](pyproject.toml) | Project configuration and dependencies. |
| [**uv.lock**](uv.lock) | Dependency lock file. |

## Setup

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) 0.13.5 or newer
and Python 3.13.17 or a newer 3.13 patch release, as defined in
[`.python-version`](.python-version).

> [!NOTE]
> Throughout these steps, text in angle brackets like `<repo-name>` is a **placeholder**. Replace it, including the `< >` brackets, with your own value. For example, `cd <repo-name>` becomes `cd mle-data-pipeline-project`.

### 1. Create the Repository from the Template

Click **Use this template** on GitHub.

When creating the repository:

- Set yourself as the **Owner**
- Choose a repository name
- Disable **Include all branches**
- Click **Create repository**

> [!IMPORTANT]
> If you are working in pairs or groups, only **one person** should complete this step.

---

### 2. Add Collaborators (Pairs/Groups Only)

If working with teammates:

1. Open the repository on GitHub
2. Go to **Settings → Collaborators**
3. Add your teammates as collaborators
4. Share the repository link with your team

Teammates should accept the invitation before continuing.

---

### 3. Clone the Repository

Copy the SSH URL from the **Code** button on GitHub, then run:

```bash
git clone <copied-ssh-url>
```

The copied SSH URL will look like `git@github.com:<your-username>/<repo-name>.git`.

---

### 4. Move into the Project Folder and Install Dependencies

This installs all dependencies and creates a virtual environment in `.venv/`.

```bash
cd <repo-name>
uv sync
```

---

### 5. Open the Repository in VS Code

> [!NOTE]
> Make sure you open VS Code from the project root so it automatically detects the environment created by `uv sync`.

Launch VS Code in the project root folder:

```bash
code .
```

If you create a notebook to explore the data, select the Python environment created by `uv sync` as the kernel.

## Reference Solution

This repository includes a complete local reference implementation in [solution/](solution/).
Use it to compare implementation choices and expected outputs. It is one way to solve the project, not the required structure for yours.

See [solution/README.md](solution/README.md) for how to run it and how its files fit together.

## Answer the Following Questions

1. What are the steps you took to complete the project?
2. What challenges did you face?
3. What would you do differently if you had more time?

### 1. What are the steps you took to complete the project?

I followed the four steps in [project.md](project.md), which also has a detailed "Working" section for each point.

1. **Extract: download the data.** I wrote [scripts/download_data.py](scripts/download_data.py), which builds the download link for each month (Jan–Mar 2025) and saves the Parquet files to `data/raw/`. Files that are already there are skipped, so running it again is quick. Each download goes to a temporary `.part` file first, so a failed download never leaves a broken file behind.
2. **Explore the data.** In [notebooks/explore.ipynb](notebooks/explore.ipynb) I loaded the data, picked the columns I needed (`lpep_pickup_datetime` for the day, `total_amount` for revenue), and checked data types, missing values and value ranges. I found 11 trips outside Q1 2025, 478 negative amounts (mostly "No charge" and "Dispute" trips) and 254 zero amounts. From that I decided on the cleaning rules.
3. **Transform and load: build the pipeline.** I chose **ETL**: the data is small (146k rows), there is no database, and pandas is enough. I split the code into one module per stage in [src/taxi_pipeline/](src/taxi_pipeline/) (`extract.py`, `transform.py`, `load.py`), plus a shared `config.py`. The pipeline drops the out-of-period and non-positive trips, prints the row count after each rule (146,486 → 145,743, 99.5% kept), groups by pickup day, and writes `data/processed/daily_revenue.csv` and `.parquet`. The result is 90 days and **$3,401,753.17** total revenue. I added tests in [tests/test_taxi_transform.py](tests/test_taxi_transform.py) for the cleaning rules and daily totals.
4. **Orchestrate with Prefect (bonus).** In [src/taxi_pipeline/flow.py](src/taxi_pipeline/flow.py) each stage is a Prefect task, and the whole run is one flow. Only the download task has retries (3 retries, after 5, 15 and 30 s), because it is the only step that uses the network. I tested the retries by requesting a month that doesn't exist.
5. **Present the result.** I summarised the result in an 8-slide deck, [docs/green_taxi_daily_revenue.pdf](docs/green_taxi_daily_revenue.pdf), with charts of revenue per day, by weekday and by month, and the best and worst days.

**How to run it:**

```bash
uv sync
uv run scripts/download_data.py      # stage the raw files
uv run scripts/run_pipeline.py       # plain ETL pipeline
uv run scripts/run_prefect_flow.py   # same pipeline as a Prefect flow
uv run pytest                        # tests
```

### 2. What challenges did you face?

- **Dirty data.** The files are split by month, but some trips in them were picked up outside that month, a few even outside Q1 2025 (from 2024-12-25 to 2025-04-01). Without a date filter, these would have added stray days to the result. There were also negative and zero amounts, which I had to understand before deciding to drop them.
- **Deciding what "revenue" means.** `total_amount` (everything the passenger paid) and `fare_amount` (only the metered fare) give different answers. I chose `total_amount` and documented why.
- **Refunds and reversals.** The smallest amount (−$473.10) exactly cancels out the largest March trip ($473.10), so negative rows seem to reverse earlier charges. Dropping only the negative row means the original charge still counts as revenue. I kept the simple rule and documented it as a known limitation.
- **Keeping the stages consistent.** At first the download was a single script with its own settings. When I added the pipeline, I moved the settings into a shared `config.py`, so the download and the pipeline always use the same file names and folders.
- **Prefect setup.** My Prefect settings still pointed at a server from an earlier project that wasn't running, which caused connection errors and a stream of `EventsWorker failed` messages. The fix was to clear `PREFECT_API_URL` so Prefect starts its own temporary server. Running `prefect config set` afterwards wrote the old address back, so the `unset` command has to run last.
- **Prefect caching and DataFrames.** By default, Prefect tries to cache task results by hashing the inputs. That doesn't suit tasks that pass whole DataFrames around, so I turned caching off for those tasks.

### 3. What would you do differently if you had more time?

- **Handle reversals properly.** Match each negative trip to the original charge it cancels and remove both, so revenue isn't overstated.
- **Try ELT with DuckDB and dbt.** Load the raw Parquet files into DuckDB and do the cleaning and grouping in SQL models with dbt. That would scale better and let others query the raw trips.
- **Add data validation.** Check the expected columns, types and value ranges automatically when reading the data (for example with pandera), instead of relying on the one-time notebook check.
- **More tests.** Add tests for the extract and load stages and for the Prefect flow, and an end-to-end test on a small sample file.
- **Make the pipeline configurable.** Let the months and paths be set from the command line, so the same code can process any period without editing `config.py`.
- **Package the project properly.** Add a build system to `pyproject.toml` so `taxi_pipeline` installs into the environment, instead of adding `src/` to the import path in each script.
- **Schedule it.** Deploy the Prefect flow with a monthly schedule so new data is downloaded and processed automatically.
