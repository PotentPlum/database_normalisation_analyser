#!/usr/bin/env bash
set -euo pipefail

MODE=${1:-serve}

/opt/mssql/bin/sqlservr &
MSSQL_PID=$!

cleanup() {
  if kill -0 "$MSSQL_PID" >/dev/null 2>&1; then
    kill "$MSSQL_PID"
  fi
}
trap cleanup EXIT

echo "Waiting for SQL Server to start..."
until /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" > /dev/null 2>&1; do
  sleep 2
  echo "Still waiting for SQL Server to accept connections..."
done

echo "Applying operational dataset..."
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d master -i /usr/src/app/operations_dataset.sql
echo "Database and sample data are ready."

if [[ "$MODE" == "test" ]]; then
  echo "Running test audit against OperationsDemo..."
  python3 /usr/src/app/sqlserver_3nf_audit.py test
  echo "Test audit completed. Stopping SQL Server."
else
  echo "SQL Server is running. Attach with port 1433."
  wait "$MSSQL_PID"
fi
