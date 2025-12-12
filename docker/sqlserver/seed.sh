#!/bin/bash
set -euo pipefail

/opt/mssql/bin/sqlservr &
SQL_PID=$!

for _ in {1..60}; do
  if /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -Q "SELECT 1" > /dev/null 2>&1; then
    break
  fi
  sleep 2
  if ! kill -0 "$SQL_PID" 2>/dev/null; then
    echo "SQL Server exited unexpectedly." >&2
    exit 1
  fi
done

if ! /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -d master -b -i /docker-entrypoint-initdb.d/operations_dataset.sql; then
  echo "Failed to apply operations_dataset.sql" >&2
  kill "$SQL_PID"
  exit 1
fi

wait "$SQL_PID"
