# FPL Analytics Project

A modular data platform for Fantasy Premier League (FPL) analytics, built with modern data engineering best practices.

---

## Data Engineering

This project uses a robust, layered approach for data management and transformation:

### 1. **Flyway** (DDL migrations)
- Handles all database schema creation and migrations.
- Ensures reproducible, version-controlled changes to your Postgres database.
- Migration scripts are stored in `db/flyway/sql/` and managed via environment-specific configuration.
- **Environments:**  
  - **dev:** Schemas use `_dev` suffix (e.g., `raw_dev`, `staging_dev`, `mart_dev`)
  - **prod:** Schemas use no suffix (e.g., `raw`, `staging`, `mart`)
- **Usage:**
  ```bash
  python scripts/run_flyway.py migrate --env dev   # for development
  python scripts/run_flyway.py migrate --env prod  # for production
  python scripts/run_flyway.py clean --env dev     # resets dev schemas (disabled in prod)
  ```

### 2. **Python ETL Scripts** (EL in ELT)
- Extracts raw JSON data from the FPL API.
- Loads data into staging tables (`raw_staging`/`raw_staging_dev`) and then upserts into raw tables (`raw`/`raw_dev`).
- All schema names and DB connection info are set via environment variables.
- **Usage:**
  ```bash
  python scripts/run_pipeline.py dev    # for development
  python scripts/run_pipeline.py prod   # for production
  ```

### 3. **dbt** (Transformations)
- Performs all data transformations.
- Transforms raw tables into clean, standardized tables in the `staging` schema (`staging`/`staging_dev`).
- Builds analytics-ready tables in the `mart` schema (`mart`/`mart_dev`).
- Uses sources configured with environment variables for schema selection.
- **Usage:**
  ```bash
  python scripts/run_dbt.py run --env dev
  python scripts/run_dbt.py run --env prod
  ```

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

- **dev:** Uses schemas with `_dev` suffix for isolated development.
- **prod:** Uses clean schema names for production.

### Environment Variable Setup

Create `.env.dev` and `.env.prod` files in your project root with all required variables:

```env
# Example .env.dev
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_dev_password
POSTGRES_HOST=your_dev_host
POSTGRES_PORT=5432
POSTGRES_DB=postgres
FPL_PROJECT_ROOT=/absolute/path/to/project
RAW_SCHEMA=raw_dev
RAW_STAGING_SCHEMA=raw_staging_dev
STAGING_SCHEMA=staging_dev
MART_SCHEMA=mart_dev

# Example .env.prod
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_prod_password
POSTGRES_HOST=your_prod_host
POSTGRES_PORT=5432
POSTGRES_DB=postgres
FPL_PROJECT_ROOT=/absolute/path/to/project
RAW_SCHEMA=raw
RAW_STAGING_SCHEMA=raw_staging
STAGING_SCHEMA=staging
MART_SCHEMA=mart
```

Scripts and tools will automatically load the correct environment file based on the `--env` argument.

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

- Copy `.env.dev` and `.env.prod` templates to your project root and fill in your secrets and schema names.

---

## License

MIT License