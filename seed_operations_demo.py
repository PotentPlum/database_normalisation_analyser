"""Seed the OperationsDemo database into a running Docker SQL Server.

Usage is intentionally minimal to match the three-step workflow:

1. Start SQL Server via Docker (compose or `docker run`).
2. Run this script once; if the database already exists, nothing happens.
3. Run `python sqlserver_3nf_audit.py` with or without the `test` flag.
"""
from __future__ import annotations

import argparse
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

from operations_dataset_sql import OPERATIONS_DATASET_SQL


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 1433
DEFAULT_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")
DEFAULT_DRIVER = os.environ.get("MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")


def ensure_driver_available(driver: str) -> None:
    try:
        import pyodbc
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guardrail
        raise SystemExit(
            "pyodbc is required to talk to SQL Server. Install it with 'pip install pyodbc'"
        ) from exc

    available = set(pyodbc.drivers())
    if driver not in available:
        if available:
            formatted = ", ".join(sorted(available))
            raise SystemExit(
                f"ODBC driver '{driver}' not installed. Installed drivers: {formatted}"
            )
        raise SystemExit(
            "No SQL Server ODBC drivers detected. Install Microsoft ODBC Driver 18 (or 17) for SQL Server."
        )


def build_engine(host: str, port: int, password: str, driver: str):
    url = (
        "mssql+pyodbc://sa:" + quote_plus(password)
        + f"@{host}:{port}/master?driver="
        + quote_plus(driver)
        + "&TrustServerCertificate=yes"
    )
    return create_engine(url, connect_args={"timeout": 30})


def database_exists(engine) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("SELECT db_id('OperationsDemo')"), {}).scalar() is not None


def split_batches(sql_text: str):
    batch = []
    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            if batch:
                yield "\n".join(batch)
                batch = []
        else:
            batch.append(line)
    if batch:
        yield "\n".join(batch)


def seed(engine) -> None:
    if database_exists(engine):
        print("OperationsDemo already present; nothing to do.")
        return

    print("Seeding OperationsDemo...")
    # SQL Server forbids CREATE/DROP/ALTER DATABASE inside explicit transactions,
    # so run the batches with autocommit enabled.
    autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    with autocommit_engine.connect() as conn:
        for i, batch in enumerate(split_batches(OPERATIONS_DATASET_SQL), start=1):
            trimmed = batch.strip()
            if not trimmed:
                continue
            print(f"Executing batch {i}...", flush=True)
            conn.exec_driver_sql(trimmed)
    print("Seeding complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the OperationsDemo database into Docker SQL Server.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="SQL Server host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="SQL Server port (default: %(default)s)")
    parser.add_argument(
        "--password", default=DEFAULT_PASSWORD, help="SA password (default: env MSSQL_SA_PASSWORD or YourStrong!Passw0rd)")
    parser.add_argument(
        "--driver",
        default=DEFAULT_DRIVER,
        help="ODBC driver name (default: env MSSQL_ODBC_DRIVER or 'ODBC Driver 18 for SQL Server')",
    )

    args = parser.parse_args()
    ensure_driver_available(args.driver)
    engine = build_engine(args.host, args.port, args.password, args.driver)
    seed(engine)


if __name__ == "__main__":
    main()
