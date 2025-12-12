# SQL Server 3NF Audit & Normalization Proposal Tool

This repository contains a single Python script that profiles SQL Server tables, discovers candidate keys and functional dependencies (FDs), flags 2NF/3NF risks, and generates **diagnostic reports only**—no DDL is produced. The tool is designed for data engineers who want explainable evidence before considering any schema changes.

## What the script does
- Connects to SQL Server 2016+ via **SQLAlchemy + pyodbc**.
- Profiles each in-scope table: row counts, column-level null/distinct stats, and a ranked determinant pool.
- Evaluates candidate keys (up to 3 columns) with strict null handling.
- Discovers functional dependencies with determinants up to size 3, capturing coverage, violation rates, and sample conflicting values.
- Identifies potential 2NF/3NF issues based on a working key and proposes **decomposition ideas only**—no automatic DDL.
- Writes machine-readable JSON plus human-readable Markdown reports under timestamped output folders.

## What the script does **not** do
- No CREATE/ALTER/DROP statements; it only reads data and writes diagnostics.
- No cross-table inference; each table is assessed independently.
- Results are approximate when sampling is used; human review is expected.

## Quick start
1. Ensure Python 3.10+ with `sqlalchemy` and `pyodbc` installed.
2. Edit `CONFIG` in `sqlserver_3nf_audit.py`:
   - Add a source entry with a valid ODBC connection string.
   - Adjust scope filters (schema/table regex or allowlist) as needed.
3. Run: `python sqlserver_3nf_audit.py`.
   - Use `python sqlserver_3nf_audit.py test` to point at the local dockerized SQL Server fixture (see below).
4. Review outputs in `output/run_YYYYMMDD_HHMMSS/`.

## Dockerized SQL Server test fixture
To exercise the audit tool against a realistic operational schema (20 interrelated tables, mixed normalization quality, and multi-million-row tables), a ready-to-run SQL Server service is provided via docker compose. No Dockerfile or SQL mounting is required.

### Three-step flow
1. **Start SQL Server via compose** (set `MSSQL_SA_PASSWORD` if you want something other than the default):
   ```bash
   docker compose up -d sqlserver
   ```

2. **Seed from the host** using the Python seeder; it targets `localhost:1433` and skips work if `OperationsDemo` already exists:
   ```bash
   python seed_operations_demo.py --password "${MSSQL_SA_PASSWORD:-YourStrong!Passw0rd}"
   ```

3. **Run the audit script**:
   - `python sqlserver_3nf_audit.py test` targets the dockerized SQL Server.
   - `python sqlserver_3nf_audit.py` uses whatever sources are configured in `CONFIG`.

When the container is running, you can connect directly with:

```
mssql+pyodbc://sa:YourStrong!Passw0rd@localhost:1433/OperationsDemo?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

> The dataset intentionally contains some denormalized columns (e.g., duplicated customer and warehouse attributes) to surface 2NF/3NF findings during testing.

## Configuration overview
All settings live inside `CONFIG` at the top of `sqlserver_3nf_audit.py`:

- `SOURCES`: list of `{name, sqlalchemy_url}` entries using the `mssql+pyodbc` dialect.
- `SCOPE`: optional include/exclude regex for schemas/tables plus a `TABLE_ALLOWLIST`.
- `OVERRIDES`: force working keys, force-include columns, or ignore columns per table.
- `LIMITS`: search bounds (max determinant size, pool size, dependents tested, confirmation counts).
- `SAMPLING`: triage sampling thresholds and row limits for full scans.
- `THRESHOLDS`: acceptance rules for keys/FDs and a minimum row count for confident calls.
- `OUTPUT`: base path for artifacts (timestamped subfolders are created automatically).
- `EXCLUDE_COLUMNS_REGEX` and `BLOB_TYPES`: defaults to skip audit/ETL columns and blob-ish types.

## Sampling and confirmation
- Tables under `FULL_SCAN_MAX_ROWS` are fully scanned; larger tables use `TABLESAMPLE` to hit `SAMPLE_TARGET_ROWS` within `SAMPLE_MIN_PCT`/`SAMPLE_MAX_PCT` bounds.
- Key/FD tests honor strict null handling: for `X -> Y`, only rows where all `X` and `Y` are **not null** are evaluated, and coverage is reported as tested_rows / total_rows.
- Accepted FDs obey thresholds on coverage and violation rates; supersets are pruned when subsets suffice.

## Outputs
Generated under `output/run_YYYYMMDD_HHMMSS/`:

- `manifest.json`: run-level summary of processed tables and any errors.
- `summary.csv`: headline stats per table (row count, working key, number of accepted FDs).
- `source_<SourceName>/<schema>.<table>/`:
  - `profile.json`: column metadata and determinant pool.
  - `key_candidates.json`: tested key combos with duplication/null metrics.
  - `fds.json`: accepted functional dependencies with evidence.
  - `proposals.json`: 3NF decomposition suggestions with confidence and notes.
  - `report.md`: human-readable summary of key findings and proposals.

## Interpreting FD metrics
- `coverage_pct`: share of total rows used in the test (after null filtering). Low coverage means results are less reliable.
- `violating_groups_pct`: percentage of determinant groups where dependent values conflict.
- `violating_rows_pct`: percentage of tested rows belonging to violating groups.
- Strong FDs meet coverage and violation thresholds; otherwise they are informational only.

## Limitations and validation
- Sampling introduces approximation; rerun with tighter limits if critical decisions depend on the findings.
- Blob-like columns and common audit fields are skipped by default to avoid noise; override as needed.
- The tool assumes read-only access; ensure the service account can `SELECT` and `APPROX_COUNT_DISTINCT`.
- Always validate proposals with domain experts before altering schemas.

## Troubleshooting
- **ODBC driver errors**: install an appropriate SQL Server ODBC Driver (17+ recommended) and update the connection string.
- **Authentication failures**: switch between Trusted_Connection and explicit credentials as required; confirm network/firewall access.
- **Performance**: adjust sampling percentages, decrease determinant pool size, or restrict scope to specific tables.
- **Permissions**: the account must read system catalogs (`sys.tables`, `sys.columns`) and target tables; no write privileges are needed.

## How to extend
- Add richer heuristics to `DeterminantSelector` for domain-specific keys.
- Tighten thresholds or expand determinant size if your data model requires it.
- Integrate results into existing governance dashboards by consuming the JSON artifacts.
