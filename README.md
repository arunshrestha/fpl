# FPL Analytics Project

A modular data platform for Fantasy Premier League (FPL) analytics, built with modern data engineering best practices.

---

## Data Engineering

This project uses a robust, layered approach for data management and transformation:

### 1. **Flyway** (DDL migrations)
- Handles all database schema creation and migrations.
- Ensures reproducible, version-controlled changes to your Postgres database.
- Migration scripts are stored in `db/flyway/sql/` and managed via environment-specific configuration.

- **Usage:**
  ```bash
  python -m scripts.run_flyway # for local development
  ```

### 2. **Python ETL Scripts** (EL in ELT)
- Extracts raw JSON data from the FPL API.
- Loads data into staging tables (`raw_staging_temp`) and then upserts into raw tables (`raw`).
- All DB connection info are set via environment variables.
- All schema name info are set via schemas.py file.

- **Usage:**
  ```bash
  python -m scripts.run_pipeline # for local development
  ```

### 3. **dbt** (Transformations)
- Performs all data transformations.
- Transforms raw tables into clean, standardized tables in the `staging` schema (`staging`).
- Builds analytics-ready tables in the `mart` schema (`mart`).
- Uses sources configured with environment variables for schema selection.

- **Usage:**
  ```bash
  python -m scripts.run_dbt # for local development
  ```
---
## Staging conventions

- Staging models: canonical, typed representation of raw JSON with:
  - scalar fields extracted and cast
  - original json/jsonb retained for audit
  - repeating arrays normalized to one-row-per-parent+element models (e.g., `stg_gameweek_chip_plays`)
- Mart models: business-facing denormalizations, pivots, aggregations.

---

## Analytics

*Coming soon.*

- Python scripts and Jupyter notebooks will be used for data exploration, analysis, and reporting.

---

## Web App / Frontend

*TODO.*

- Interactive dashboards and web applications will be added in future versions.

---

## Environments

This project supports multiple environments for safe development and production deployment:

- **local:** Uses local postgres db setup. See .env.example for env vairables that need to be set
- **staging:** Uses supabase db and Github Actions for CI/CD.
- **prod:** Uses supabase db and Github Actions for CI/CD.

### Environment Variable Setup

1. Copy the `.env.example` file to `.env.local`:
   ```bash
   cp .env.example .env.local
   ```

2. Open `.env.local` and replace the placeholder values with your local Postgres credentials:
   - `DB_HOST` - your local Postgres host (typically `localhost`)
   - `DB_PORT` - your local Postgres port (typically `5432`)
   - `DB_USER` - your Postgres username
   - `DB_PASSWORD` - your Postgres password
   - `DB_NAME` - your local database name
   - Any other schema or configuration variables as needed

---

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd fpl
```

### 2. Create a Python virtual environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Install Flyway (for DDL migrations)

```bash
brew install flyway
```

### 4. Set up environment variables

- Create a `.env.local` file to your project root and fill in your secrets and schema names.

### 5. Install dbt

```bash
cd dbt
dbt deps
```

dbt is installed via `pip` in your virtual environment (see `requirements.txt`).

---

## CI/CD Pipeline

This project uses **GitHub Actions** to automate deployments across environments:

### Workflow Overview

- **Local Development**
  - Run scripts manually: `python -m scripts.run_flyway`, `python -m scripts.run_pipeline`, `python -m scripts.run_dbt`
  - Test changes thoroughly before pushing

- **Staging Environment** (triggered on push to `staging` branch)
  - Automatically applies Flyway DDL migrations
  - Runs dbt transformations to update `staging` and `mart` schemas
  - Validates changes in a safe environment before production

- **Production Environment** (triggered on merge to `main` branch)
  - Automatically applies Flyway DDL migrations
  - Runs dbt transformations to update `staging` and `mart` schemas
  - Deploys validated changes to production

### Scheduled Jobs

- **Python ETL Pipeline** runs on a cron schedule in both staging and prod environments
  - Extracts raw JSON data from the FPL API
  - Loads data into raw tables
  - Keeps your data fresh and up-to-date automatically

---

## License

MIT License