#!/usr/bin/env bash
set -euo pipefail

/opt/mssql/bin/sqlservr &
MSSQL_PID=$!

echo "Waiting for SQL Server to start..."
until /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" > /dev/null 2>&1; do
  sleep 2
  echo "Still waiting for SQL Server to accept connections..."
done

echo "Applying operational dataset..."
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d master -i /usr/src/app/operations_dataset.sql

echo "Database and sample data are ready."
wait $MSSQL_PID
