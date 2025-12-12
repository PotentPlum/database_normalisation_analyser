FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y \
    MSSQL_PID=Developer

COPY operations_dataset.sql /docker-entrypoint-initdb.d/operations_dataset.sql
COPY docker/sqlserver/seed.sh /usr/local/bin/seed.sh

USER root
RUN chmod +x /usr/local/bin/seed.sh

USER mssql
EXPOSE 1433

CMD ["/usr/local/bin/seed.sh"]
